using Npgsql;
using static Bybit.Api.Enums.BybitInterval;
using Bybit.Api.Enums;
using System.Diagnostics;
using System.Data.Common;
using Bybit.Api.Market;



class DbRelation
{
    private string _connectionString = "Host=localhost;Port=5432;Database=candles_repo;Username=super_user;Password=qwerty";
    private Parser _parser;
    private static SemaphoreSlim _semaphore = new(11, 11);
    private List<string> _tablesNames = new();
    private List<KeyValuePair<BybitInterval, string>> intervals = new()
    {
        new(OneDay, "1D"),
        new(FourHours, "4H"),
        new(TwoHours, "2H"),
        new(FifteenMinutes, "15M"),
        new(FiveMinutes, "5M"),
        new(OneMinute, "1M"),
    };
    public DbRelation(Parser parser)
    {
        _parser = parser;
    }
    public async Task InitTables(List<string>? tablesNames = null)              // null for all futures from launch
    {
        await EnsureExistingTables();
        await _parser.GetFuturesNamesAsync();
        foreach (var tokenName in tablesNames ?? _parser.futuresList)
        {
            if (tablesNames is null && !_tablesNames.Contains(tokenName))
            {
                await CreateTable(tokenName);
                foreach (var interval in intervals)
                {
                    var data = (await _parser.GetChartFromLaunch(tokenName, interval.Key)).GetArmedKlines();
                    if (data.Count != 0) await InsertData(data, interval.Value, $"{tokenName}");
                }
                await FillTablesGaps(new() { tokenName });
                await FillToNow(tokenName);
            }
            else
            {
                await FillTablesGaps(new() { tokenName });
                await FillToNow(tokenName);
            }
        }
    }
    public async Task<bool> InsertData(List<ArmedBybitMarketKline> klines, string duration, string tokenName)
    {
        tokenName = $"\"{tokenName}\"";
        DbRequest dbRequest = new(tokenName, duration, klines.Count, 0, klines.Count - 1);
        List<DateTime> dataToAdd = klines.Select(candle => candle.OpenTime).ToList();
        int result = 0;
        Stopwatch sw = new();
        if(dbRequest.TasksCount == 1)
        {
            try
            {
                sw.Start();
                result += await InsertDataForTerm(klines, 0, dbRequest, dataToAdd);
                sw.Stop();
                Console.WriteLine($"{result} records added, {Math.Round((double)sw.ElapsedMilliseconds / 1000, 2)} sec.");
                return true;
            }
            catch
            {
                throw new Exception();
            }
        }
        Task<int>[] fillers = new Task<int>[dbRequest.TasksCount];
        sw.Start();
        for (int i = 0; i < dbRequest.TasksCount; i++)
        {
            int step = i;
            fillers[step] = Task.Run(() => InsertDataForTerm(klines, dbRequest.FunctionsStartPoints[step], dbRequest, dataToAdd));
        }
        await Task.WhenAll(fillers);
        sw.Stop();
        foreach (var filler in fillers) result += filler.Result;
        if (!dataToAdd.All(data => data == DateTime.MinValue)) throw new Exception("not full data added");
        Console.WriteLine($"{result} records added, {Math.Round((double)sw.ElapsedMilliseconds / 1000, 2)} sec.");
        return true;
    }

    private async Task<int> InsertDataForTerm(List<ArmedBybitMarketKline> klines, int startPoint, DbRequest request, List<DateTime> dataToAdd)
    {
        await _semaphore.WaitAsync();
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            int res = 0;
            await connection.OpenAsync();
            for (int i = startPoint; i < klines.Count; i++)
            {
                int n = i;
                string query = @$"INSERT INTO {request.TokenName}(open_time, duration, open_price, high_price, low_price, close_price, volume, change_percent,
                macd_indicator, boll_indicator, rsi_indicator, sma_indicator, upper_tail, down_tail) VALUES
                (@dateTime, CAST(@duration AS interval), @openPrice, @highPrice, @lowPrice, @closePrice, @volume, @changePercent,
                @macd, @boll, @rsi, @sma, @upperTail, @downTail);";
                try
                {
                    using (var cmd = new NpgsqlCommand(query, connection))
                    {
                        var options = cmd.Parameters;
                        options.AddWithValue("dateTime", klines[n].OpenTime);
                        options.AddWithValue("duration", request.Duration);
                        options.AddWithValue("openPrice", klines[n].OpenPrice);
                        options.AddWithValue("highPrice", klines[n].HighPrice);
                        options.AddWithValue("lowPrice", klines[n].LowPrice);
                        options.AddWithValue("closePrice", klines[n].ClosePrice);
                        options.AddWithValue("volume", klines[n].Volume);
                        options.AddWithValue("changePercent", klines[n].ChangePercent);
                        options.AddWithValue("macd", NpgsqlTypes.NpgsqlDbType.Unknown, $"({klines[n].MACD.Signal},{klines[n].MACD.Fast},{klines[n].MACD.Slow})");
                        options.AddWithValue("boll", NpgsqlTypes.NpgsqlDbType.Unknown, $"({klines[n].BOLL.MiddleBand},{klines[n].BOLL.UpperBand},{klines[n].BOLL.LowerBand})");
                        options.AddWithValue("rsi", klines[n].RSI);
                        options.AddWithValue("sma", klines[n].SMA);
                        options.AddWithValue("upperTail", klines[n].UpperTail);
                        options.AddWithValue("downTail", klines[n].DownTail);
                        res += await cmd.ExecuteNonQueryAsync();
                        dataToAdd[dataToAdd.IndexOf(klines[n].OpenTime)] = DateTime.MinValue;
                    }
                }
                catch (Exception ex)
                {
                    if (ex is DbException dbEx)
                    {
                        // Console.WriteLine(dbEx.Message);
                        await connection.CloseAsync();                  // PRIMARY KEY
                        _semaphore.Release();
                        return res;
                    }
                    else throw new Exception();
                }
            }
            await connection.CloseAsync();
            _semaphore.Release();
            return res;
        }
    }
 
    public async Task FillTablesGaps(List<string>? tables = null) // all existing or concrete //Заполняет разрывы в open_time(Все интервалы)
    {
        await EnsureExistingTables();
        if (tables is not null)
        {
            foreach (string table in tables)
            {
                var gapsToFill = await CheckTableIntegrity(table);
                await FillConcreteGaps(gapsToFill, table);
            }
            Console.WriteLine("\tAll gaps filled.");
            return;
        }
        else
        {
            foreach (string tableName in _tablesNames)
            {
                var gapsToFill = await CheckTableIntegrity(tableName);
                await FillConcreteGaps(gapsToFill, tableName);
            }
        }
        Console.WriteLine("\tAll gaps filled.");
    }

    private async Task EnsureExistingTables()
    {
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            string tablesNamesQuery = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            await conn.OpenAsync();
            using (NpgsqlCommand cmd = new(tablesNamesQuery, conn))
            {
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        _tablesNames.Add(reader.GetString(0));
                    }
                }
                if (_tablesNames.Count == 0) throw new Exception("No tables in db");
            }
        }
    }

    private async Task FillConcreteGaps(List<(DateTime start, DateTime end, BybitInterval spacing)> gapsToFill, string tokenName)    // lastDate, interval, currencyName
    {
        foreach (var gap in gapsToFill)
        {
            DateTime start = gap.start;
            DateTime end = gap.end;
            int interval = (int)gap.spacing;
            var data = (await _parser.GetChartForTerm(tokenName, (BybitInterval)interval, start.AddSeconds(interval), end.AddSeconds(-interval))).GetArmedKlines();
            await InsertData(data, intervals.Where(interval => interval.Key == gap.spacing).First().Value, $"{tokenName}");
        }
    }

    public async Task<List<(DateTime start, DateTime end, BybitInterval spacing)>> CheckTableIntegrity(string tableName) //Все разрывы open_time в таблице
    {
        Console.WriteLine($"Checking table integrity \"{tableName}\"");
        List<(DateTime start, DateTime end, BybitInterval spacing)> totalGaps = new();
        List<BybitMarketKline> klines;
        using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            foreach (var interval in intervals)
            {
                klines = new();
                string query = $"SELECT duration, open_time FROM \"{tableName}\" where duration = CAST('{interval.Value}' as interval)order by duration asc, open_time asc";
                using (NpgsqlCommand cmd = new(query, conn))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            klines.Add(new() { OpenTime = reader.GetDateTime(1) });
                        }
                    }
                }
                List<(DateTime Key, DateTime Value)> gaps;
                if (klines.Count >= 2)
                {
                    Parser.SortAndCheck(klines, interval.Key, out gaps);
                    foreach (var gap in gaps) totalGaps.Add((gap.Key, gap.Value, interval.Key));
                }
            }
        }
        return totalGaps;
    }

    public async Task FillToNow(string tableName)           // Заполняет с последнего open_time до настоящего момента(Все интервалы)
    {
        List<(DateTime Key, BybitInterval Value)> lastDates = new();
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            foreach (var interval in intervals)
            {
                string query = $"SELECT open_time, duration FROM \"{tableName}\" where duration = '{interval.Value}' order by open_time desc limit 1";
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows) lastDates.Add((DateTime.MinValue, interval.Key));
                        else
                        {
                            while (await reader.ReadAsync())
                            {
                                DateTime lastDate = reader.GetDateTime(0);
                                if ((DateTime.Now - lastDate).TotalSeconds >= (int)interval.Key) lastDates.Add((lastDate, interval.Key));
                            }
                        }
                    }
                }
            }
        }
        foreach (var dateInterval in lastDates)
        {
            List<ArmedBybitMarketKline> dataToFill;
            BybitInterval interval = dateInterval.Value;
            if (dateInterval.Key == DateTime.MinValue) dataToFill = (await _parser.GetChartFromLaunch(tableName, interval)).GetArmedKlines();
            else
            {
                DateTime dateFrom = dateInterval.Key.AddSeconds((int)interval);
                dataToFill = (await _parser.GetChartForTerm(tableName, dateInterval.Value, dateFrom, DateTime.Now)).GetArmedKlines();
            }
            await InsertData(dataToFill, intervals.Where(spacing => spacing.Key == interval).First().Value, $"{tableName}");
        }
        await FillTablesGaps(new() { tableName });
    }

    public async Task CreateTable(string tokenName)
    {
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            tokenName = $"\"{tokenName}\"";
            string query =
            @$"create table {tokenName}(
            open_time timestamp with time zone,
            duration interval,
            open_price decimal, 
            high_price decimal,
            low_price decimal,
            close_price decimal,
            volume decimal,
            change_percent decimal,
            macd_indicator macd,
            boll_indicator boll, 
            rsi_indicator decimal, 
            sma_indicator decimal,
            upper_tail decimal, 
            down_tail decimal,
            PRIMARY KEY(open_time, duration));
            ";
            using (var cmd = new NpgsqlCommand(query, connection))
            {
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }
}

public class DbRequest : ForceRequest<int>
{
    private int DataCount { get; set; } = 0;
    public string TokenName { get; set; }
    public string Duration { get; set; }
    public DbRequest(string tokenName, string duration, int dataCount, int startPoint, int endPoint) : base(startPoint, endPoint)
    {
        DataCount = dataCount;
        TokenName = tokenName;
        Duration = duration;
        SetForceOptions();
    }

    protected override void SetForceOptions()
    {
        TasksCount = Math.Min((int)Math.Ceiling((double)DataCount / 2000), Environment.ProcessorCount);
        FunctionsStartPoints = new int[TasksCount];
        for (int step = 0; step < TasksCount; step++)
        {
            FunctionsStartPoints[step] = step == 0 ? StartPoint : DataCount / TasksCount * step;
        }
    }
}
