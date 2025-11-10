using Npgsql;
using static Bybit.Api.Enums.BybitInterval;
using Bybit.Api.Enums;
using System.Diagnostics;
using System.Data.Common;
using Bybit.Api.Market;

public class DbRelation
{
    private string? _connectionString;
    private Parser _parser;
    private static SemaphoreSlim _semaphore = new(12, 12);
    private List<string> _tablesNames = new();
    public static List<string> MainFutures = new(){"BTCUSDT", "ETHUSDT", "XRPUSDT", "DOGEUSDT", "SOLUSDT", "SUIUSDT", "1000PEPEUSDT", "HYPEUSDT",
    "FARTCOINUSDT", "TRUMPUSDT", "PUMPFUNUSDT", "XPLUSDT", "WLFIUUSDT", "LINEAUSDT", "BARDUSDT" };
    private List<KeyValuePair<BybitInterval, string>> intervals = new()
    {
        new(OneDay, "1D"),
        new(FourHours, "4H"),
        new(TwoHours, "2H"),
        new(OneHour, "1H"),
        new(ThirtyMinutes, "30M"),
        new(FifteenMinutes, "15M"),
        new(FiveMinutes, "5M"),
        new(OneMinute, "1M"),
    };
    public DbRelation(Parser parser)
    {
        _parser = parser;
        _connectionString = Config.GetConfig()?.DB_CONNECTION_STRING;
    }
    public async Task InitTables(List<string>? tablesNames = null)              // null for all futures from launch
    {
        await EnsureExistingTables();
        await _parser.GetFuturesNamesAsync();
        foreach (var tokenName in tablesNames ?? _parser.futuresList)
        {
            await InitTable(tokenName);
        }
    }

    public async Task InitTable(string tokenName)
    {
        await EnsureExistingTables();
        if (!_tablesNames.Contains(tokenName)) await CreateTable(tokenName);
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            foreach (var interval in intervals)
            {
                string query = $"SELECT duration, open_time FROM \"{tokenName}\" where duration = CAST('{interval.Value}' as interval)order by duration asc, open_time asc";
                using (var cmd = new NpgsqlCommand(query, conn))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            var data = (await _parser.GetChartFromLaunch(tokenName, interval.Key)).GetArmedKlines();
                            if (data.Count != 0) await InsertData(data, interval.Value, $"{tokenName}");
                        }
                    }
                }
            }
            if (!await FillToNow(tokenName)) await FillToNow(tokenName);
            await FillTableGaps(tokenName);
        }
    }

    private async Task<bool> InsertData(List<ArmedBybitMarketKline> klines, string duration, string tokenName)
    {
        tokenName = $"\"{tokenName}\"";
        DbRequest dbRequest = new(tokenName, duration, klines.Count, 0, klines.Count - 1);
        List<DateTime> dataToAdd = klines.Select(candle => candle.OpenTime).ToList();
        int result = 0;
        Stopwatch sw = new();
        if (dbRequest.TasksCount == 1)
        {
            try
            {
                sw.Start();
                Console.Write("Inserting to database: ");
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
        Console.Write("Inserting to database: ");
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
                var k = klines[n];
                string query = @$"INSERT INTO {request.TokenName}(open_time, duration, open_price, high_price, low_price, close_price,
                volume, quote_volume, change_percent, upper_tail, down_tail) VALUES
                (@dateTime, CAST(@duration AS interval), @openPrice, @highPrice, @lowPrice,
                @closePrice, @volume, @quote_volume, @changePercent, @upperTail, @downTail)";
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
                        options.AddWithValue("quote_volume", klines[n].QuoteVolume);
                        options.AddWithValue("changePercent", klines[n].ChangePercent);
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

    private async Task FillTableGaps(string tableName) // all existing or concrete //Заполняет разрывы в open_time(Все интервалы)
    {
        await EnsureExistingTables();
        var gapsToFill = await EnsureTableIntegrity(tableName);
        if (gapsToFill.Count != 0)
        {
            await FillConcreteGaps(gapsToFill, tableName);
            Console.WriteLine("\tAll gaps filled.");
        }
        else Console.WriteLine("No gaps.");
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
            }
        }
    }

    private async Task FillConcreteGaps(List<(DateTime start, DateTime end, BybitInterval spacing)> gapsToFill, string tokenName)    // lastDate, interval, currencyName
    {
        Console.WriteLine("Filling gaps:\n");
        foreach (var gap in gapsToFill)
        {
            DateTime start = gap.start;
            DateTime end = gap.end;
            int interval = (int)gap.spacing;
            var data = (await _parser.GetChartForTerm(tokenName, (BybitInterval)interval, start.AddSeconds(interval), end.AddSeconds(-interval))).GetArmedKlines();
            await InsertData(data, intervals.Where(interval => interval.Key == gap.spacing).First().Value, $"{tokenName}");
        }
    }

    private async Task<List<(DateTime start, DateTime end, BybitInterval spacing)>> EnsureTableIntegrity(string tableName) //Все разрывы open_time в таблице
    {
        Console.WriteLine($"Checking table integrity \"{tableName}\"");
        List<(DateTime start, DateTime end, BybitInterval spacing)> totalGaps = new();
        List<BybitMarketKline> klines;
        using (NpgsqlConnection conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            foreach (var interval in intervals)
            {
                int capacity = 0;
                string capacityQuery = $"SELECT count(*) from \"{tableName}\" where duration = CAST('{interval.Value}' as interval)";
                using (NpgsqlCommand cmd = new(capacityQuery, conn))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync()) capacity = reader.GetInt32(0);
                    }
                }
                klines = new(capacity);
                string query = $"SELECT duration, open_time FROM \"{tableName}\" where duration = CAST('{interval.Value}' as interval)order by open_time asc";
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

    private async Task<bool> FillToNow(string tableName)           // Заполняет с последнего open_time до настоящего момента(Все интервалы)
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
        return lastDates.Count > 0 ? false : true;
    }

    private async Task CreateTable(string tokenName)
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
            quote_volume decimal,
            change_percent decimal,
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
    public async Task<List<ArmedBybitMarketKline>> GetCandles(string tokenName, BybitInterval interval, int limit = 0)
    {
        List<BybitMarketKline> klines = new(0);
        using (NpgsqlConnection connection = new(_connectionString))
        {
            string _interval = intervals.Where(i => i.Key == interval).First().Value;
            await connection.OpenAsync();
            int capacity = 0;
            if (limit == 0)
            {
                string capacityQuery = $"SELECT count(*) from \"{tokenName}\" where duration = CAST('{_interval}' as interval)";
                using (NpgsqlCommand cmd = new(capacityQuery, connection))
                {
                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync()) capacity = reader.GetInt32(0);
                    }
                }
                klines = new(capacity);
            }
            string query = $"SELECT * FROM \"{tokenName}\" where duration = CAST('{_interval}' as interval) order by open_time desc ";
            if (limit > 0)
            {
                query += $"limit {limit}";
                klines = new(limit);
            }
            using (NpgsqlCommand cmd = new(query, connection))
            {
                using (NpgsqlDataReader reader = await cmd.ExecuteReaderAsync())
                {
                    if (!reader.HasRows) throw new Exception("NO ROWS IN TABLE");
                    Console.WriteLine("executing query");
                    while (await reader.ReadAsync())
                    {
                        klines.Add(new()
                        {
                            OpenTime = reader.GetDateTime(0),
                            OpenPrice = reader.GetDecimal(2),
                            HighPrice = reader.GetDecimal(3),
                            LowPrice = reader.GetDecimal(4),
                            ClosePrice = reader.GetDecimal(5),
                            Volume = reader.GetDecimal(6),
                            QuoteVolume = reader.GetDecimal(7)
                        });
                    }
                }
                List<(DateTime, DateTime)> gaps;
                klines = Parser.SortAndCheck(klines, interval, out gaps);
                if (gaps.Count == 0) return klines.GetArmedKlines();
                else
                {
                    await InitTable(tokenName);
                    return await GetCandles(tokenName, interval, limit);
                }
            }
        }
    }

    public async Task SetIndicators(string tokenName, BybitInterval interval)
    {
        await InitTable(tokenName);
        var data = await GetCandles(tokenName, interval, 10000);
        data.GetArmedKlines();
        foreach(var i in data)
        {
            Console.WriteLine($"{i.OpenTime} - {i.RSI}");
        }
    }
}

