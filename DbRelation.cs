using Npgsql;
using static Bybit.Api.Enums.BybitInterval;
using Bybit.Api.Enums;
using System.Diagnostics;
using System.Data.Common;



class DbRelation
{
    private string _connectionString = "Host=localhost;Port=5432;Database=candles_repo;Username=super_user;Password=qwerty";
    private Parser _parser;
    private static SemaphoreSlim _semaphore = new(11, 11);
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
    public async Task InitTableDataFromLaunch()
    {
        await _parser.GetFuturesNamesAsync();
        foreach (var tokenName in _parser.futuresList)
        {
            await CreateTable($"\"{tokenName}\"");
            foreach (var interval in intervals)
            {
                var data = (await _parser.GetChartFromLaunch(tokenName, interval.Key)).GetArmedKlines();
                if (data.Count != 0) await InsertData(data, interval.Value, $"\"{tokenName}\"");
            }
        }
    }
    private async Task<bool> InsertData(List<ArmedBybitMarketKline> klines, string duration, string tokenName)
    {
        DbRequest dbRequest = new(klines.Count, 0, klines.Count - 1);
        int result = 0;
        Stopwatch sw = new();
        if(dbRequest.TasksCount == 1)
        {
            try
            {
                sw.Start();
                result += await InsertDataForTerm(klines, duration, tokenName, 0);
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
            fillers[step] = Task.Run(() => InsertDataForTerm(klines, duration, tokenName, dbRequest.FunctionsStartPoints[step]));
        }
        await Task.WhenAll(fillers);
        sw.Stop();
        foreach (var filler in fillers) result += filler.Result;
        Console.WriteLine($"{result} records added, {Math.Round((double)sw.ElapsedMilliseconds / 1000, 2)} sec.");
        return true;
    }

    private async Task<int> InsertDataForTerm(List<ArmedBybitMarketKline> klines, string duration, string tokenName, int startPoint)
    {
        await _semaphore.WaitAsync();
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            int res = 0;
            await connection.OpenAsync();
            for (int i = startPoint; i < klines.Count; i++)
            {
                string query = @$"INSERT INTO {tokenName}(open_time, duration, open_price, high_price, low_price, close_price, volume, change_percent,
                macd_indicator, boll_indicator, rsi_indicator, sma_indicator, upper_tail, down_tail) VALUES
                (@dateTime, CAST(@duration AS interval), @openPrice, @highPrice, @lowPrice, @closePrice, @volume, @changePercent,
                @macd, @boll, @rsi, @sma, @upperTail, @downTail);";
                try
                {
                    using (var cmd = new NpgsqlCommand(query, connection))
                    {
                        var options = cmd.Parameters;
                        options.AddWithValue("dateTime", klines[i].OpenTime);
                        options.AddWithValue("duration", duration);
                        options.AddWithValue("openPrice", klines[i].OpenPrice);
                        options.AddWithValue("highPrice", klines[i].HighPrice);
                        options.AddWithValue("lowPrice", klines[i].LowPrice);
                        options.AddWithValue("closePrice", klines[i].ClosePrice);
                        options.AddWithValue("volume", klines[i].Volume);
                        options.AddWithValue("changePercent", klines[i].ChangePercent);
                        options.AddWithValue("macd", NpgsqlTypes.NpgsqlDbType.Unknown, $"({klines[i].MACD.Signal},{klines[i].MACD.Fast},{klines[i].MACD.Slow})");
                        options.AddWithValue("boll", NpgsqlTypes.NpgsqlDbType.Unknown, $"({klines[i].BOLL.MiddleBand},{klines[i].BOLL.UpperBand},{klines[i].BOLL.LowerBand})");
                        options.AddWithValue("rsi", klines[i].RSI);
                        options.AddWithValue("sma", klines[i].SMA);
                        options.AddWithValue("upperTail", klines[i].UpperTail);
                        options.AddWithValue("downTail", klines[i].DownTail);
                        res += await cmd.ExecuteNonQueryAsync();
                    }
                }
                catch (Exception ex)
                {
                    if (ex is DbException dbEx && dbEx.ErrorCode == -2147467259) //PRIMARY KEY ERROR
                    {
                        await connection.CloseAsync();
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

    private async Task CreateTable(string tokenName)
    {
        using (var connection = new NpgsqlConnection(_connectionString))
        {
            await connection.OpenAsync();
            string query =
            @$"create table {tokenName}(
            open_time timestamp,
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
    public DbRequest(int dataCount, int startPoint, int endPoint) : base(startPoint, endPoint)
    {
        DataCount = dataCount;
        SetForceOptions();
    }

    protected override void SetForceOptions()
    {
        TasksCount = Math.Min((int)Math.Ceiling((double)DataCount / 2000), Environment.ProcessorCount);
        FunctionsStartPoints = new int[TasksCount];
        for(int step = 0; step < TasksCount; step++)
        {
            FunctionsStartPoints[step] = step == 0 ? StartPoint : DataCount / TasksCount * step;
        }
    }
}