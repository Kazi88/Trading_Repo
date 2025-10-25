using System.Diagnostics;
using ApiSharp.Authentication;
using Bybit.Api;
using Bybit.Api.Enums;
using Bybit.Api.Market;
using Bybit.Net.Clients;
using Bybit.Net.Enums;
using InfluxDB.Client.Core.Exceptions;

public class Parser
{
    private static Parser _parser = null!;
    public BybitRestApiClient _apiClient = null!;
    private BybitRestClient _restClient = null!;
    private readonly object _locker = new();
    public List<string> futuresList = new(644);
    private Parser(bool isTestNet)
    {
        string BYBIT_API_KEY = "";
        string BYBIT_API_SECRET = "";
        Config? configValues = Config.GetConfig();
        if(configValues is not null)
        {
            BYBIT_API_KEY = configValues.BYBIT_API_KEY!;
            BYBIT_API_SECRET = configValues.BYBIT_API_SECRET!;
        }
        BybitRestApiClientOptions apiOptions = new(new ApiCredentials(BYBIT_API_KEY, BYBIT_API_SECRET))
        {
            BaseAddress = isTestNet ? BybitAddress.TestNet.RestApiAddress : BybitAddress.MainNet.RestApiAddress,
        };
        apiOptions.HttpOptions.RequestTimeout = TimeSpan.FromSeconds(5);
        _apiClient = new(apiOptions);
        _restClient = new();
    }

    public static Parser GetParser(bool isTestNet = false)
    {
        if (_parser is null) return new Parser(isTestNet);
        else return _parser;
    }

    public async Task GetFuturesNamesAsync(string? cursor = null)
    {
        var request = await _restClient.V5Api.ExchangeData.GetLinearInverseSymbolsAsync(Category.Linear, cursor: cursor);
        if (request is not null)
        {
            foreach (var future in request.Data.List)
            {
                if (future.Status == SymbolStatus.Trading) futuresList.Add(future.Name);
            }
            if (string.IsNullOrEmpty(request.Data.NextPageCursor))
            {
                return;
            }
            else
            {
                await GetFuturesNamesAsync(request.Data?.NextPageCursor);
            }
        }
    }

    public async Task<BybitMarketKline> GetCurrentCandle(string currencyName, BybitInterval interval)
    {
        var a = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, currencyName, interval, DateTime.UtcNow.AddMinutes(-(int)interval / 60), DateTime.UtcNow, 1);
        return a.Data.Count != 0 ? a.Data.First() : new BybitMarketKline();
    }

    public async Task<BybitMarketKline> GetPreviousCandle(string currencyName, BybitInterval interval)
    {
        var a = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, currencyName, interval, DateTime.Now.AddMinutes(-(int)interval / 60 * 2), DateTime.Now.AddMinutes(-(int)interval / 60), 1);
        return a.Data.Count != 0 ? a.Data.Last() : new BybitMarketKline();
    }

    public async Task<List<BybitMarketKline>> GetChartFromLaunch(string currencyName, BybitInterval interval)
    {
        var launch = (await _apiClient.Market.GetLinearInstrumentsAsync(currencyName))?.Data?.First()?.LaunchTime;
        DateTime launchDate = launch is not null ? launch.Value.AddDays(-1).ToUniversalTime() : DateTime.MinValue;
        ChartRequest request = new(currencyName, interval, launchDate, DateTime.UtcNow);
        List<BybitMarketKline> marketKlines = new(request.Capacity);
        return await GetChartForTerm(request, marketKlines);
    }

    public async Task<List<BybitMarketKline>> GetChartForTerm(string currencyName, BybitInterval interval, DateTime start, DateTime end)
    {
        ChartRequest request = new(currencyName, interval, start, end);
        List<BybitMarketKline> marketKlines = new(request.Capacity);
        return await GetChartForTerm(request, marketKlines);
    }

    private async Task<List<BybitMarketKline>> GetChartForTerm(ChartRequest request, List<BybitMarketKline> marketKlines)
    {
        DateTime startTime = DateTime.UtcNow;
        Stopwatch sw = new();
        if (request.TasksCount == 0) // interval - week or month OR small timeframe
        {
            try
            {
                Console.Write($"|{request.CurrencyName} - {request.IntervalToSearch}|");
                sw.Start();
                var query = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, request.CurrencyName, request.IntervalToSearch, request.StartPoint, request.EndPoint, 1000);
                sw.Stop();
                foreach (var kline in query.Data) marketKlines.Add(kline);
                marketKlines = marketKlines.OrderBy(kline => kline.OpenTime).DistinctBy(kline => kline.OpenTime).ToList();
                if(startTime < marketKlines.TakeLast(1).First().OpenTime.AddSeconds((int)request.IntervalToSearch))
                {
                    marketKlines = marketKlines.Take(marketKlines.Count - 1).ToList();
                }
                return SortAndCheck(marketKlines, request.IntervalToSearch, out _, sw.ElapsedMilliseconds);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message} GetChartForTerm ({request.IntervalToSearch}) {request.StartPoint} - {request.EndPoint}");
                Console.WriteLine($"Relaunching {request.CurrencyName}");
                await GetChartForTerm(request, marketKlines);
            }
        }
        Task[] finders = new Task[request.TasksCount];
        Console.Write($"|{request.CurrencyName} - {request.IntervalToSearch}|");
        sw.Start();
        try
        {
            for (int i = 0; i < request.TasksCount; i++)
            {
                int step = i;
                finders[step] = Task.Run(async () =>
                {
                    DateTime localStartPoint = request.FunctionsStartPoints[step];
                    DateTime functionEndPoint = step == request.TasksCount - 1 ? request.EndPoint : request.FunctionsStartPoints[step + 1];
                    var query = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, request.CurrencyName, request.IntervalToSearch, localStartPoint, functionEndPoint, 1000);
                    DateTime localEndPoint = new();
                    while (true)
                    {
                        int queryCount = query.Data.Count;
                        if (queryCount != 0)
                        {
                            if (query.Data.First().OpenTime > DateTime.UtcNow) break;
                            foreach (var i in query.Data)
                            {
                                if (i.OpenTime < localStartPoint || (query.Data.First().OpenTime == localEndPoint && query.Data.Count == 1))
                                {
                                    return;
                                }
                                lock (_locker) marketKlines.Add(i);
                            }
                        }
                        localEndPoint = query.Data.Last().OpenTime;
                        var task = Task.Run(async () =>
                        {
                            await Task.Delay(50000);
                            throw new RequestTimeoutException("Too long request time");
                        });
                        var task2 = Task.Run(async () => query = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, request.CurrencyName, request.IntervalToSearch, localStartPoint, localEndPoint, 1000));
                        await Task.WhenAny(task, task2);
                    }
                });
            }
            await Task.WhenAll(finders);
            sw.Stop();
            if (marketKlines.Count == 0) return await GetChartForTerm(request, marketKlines);
            marketKlines = marketKlines.OrderBy(kline => kline.OpenTime).DistinctBy(kline => kline.OpenTime).ToList();
            Console.WriteLine("THERE: " + startTime + " " + marketKlines.TakeLast(1).First().OpenTime.AddSeconds((int)request.IntervalToSearch));
                Thread.Sleep(10000);
            if(startTime < marketKlines.TakeLast(1).First().OpenTime.AddSeconds((int)request.IntervalToSearch))
            {
                marketKlines = marketKlines.Take(marketKlines.Count - 1).ToList();
            }
            return SortAndCheck(marketKlines, request.IntervalToSearch, out _, sw.ElapsedMilliseconds);
        }
        catch(RequestTimeoutException ex)
        {
            Console.WriteLine(ex.Message + " Parser str 174");
            Console.WriteLine($"{request.CurrencyName} TO RELAUNCHING");
            return SortAndCheck(marketKlines, request.IntervalToSearch, out _, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message + " Parser str 180");
            Console.WriteLine($"{request.CurrencyName} TO RELAUNCHING");
            return SortAndCheck(marketKlines, request.IntervalToSearch, out _, sw.ElapsedMilliseconds);
        }
    }

    public static List<BybitMarketKline> SortAndCheck(List<BybitMarketKline> marketKlines, BybitInterval interval, out List<(DateTime, DateTime)> gaps, long elapsedMilliseconds = 1000)
    {
        gaps = new(0);
        marketKlines = marketKlines.OrderBy(kline => kline.OpenTime).DistinctBy(kline => kline.OpenTime).ToList();
        int intInterval = (int)interval;
        int compareBy = interval switch
        {
            BybitInterval.OneMonth => 3,
            BybitInterval.OneWeek => 2,
            _ => 1
        };
        for (int step = 1; step < marketKlines.Count; step++)
        {
            if (compareBy == 1 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddMinutes(intInterval / 60))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 2 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddDays(7))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 3 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddMonths(1))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
        }
        if (gaps.Count == 0) Console.WriteLine($"\tData is full: {marketKlines.First().OpenTime} - {marketKlines.Last().OpenTime}, {(double)elapsedMilliseconds / 1000} sec, {marketKlines.Count} records - {interval}");
        else Console.WriteLine("Data with gaps");
        return marketKlines;
    }

    public static List<ArmedBybitMarketKline> SortAndCheck(List<ArmedBybitMarketKline> marketKlines, BybitInterval interval, out List<(DateTime, DateTime)> gaps, long elapsedMilliseconds = 1000)
    {
        gaps = new(0);
        marketKlines = marketKlines.OrderBy(kline => kline.OpenTime).DistinctBy(kline => kline.OpenTime).ToList();
        int intInterval = (int)interval;
        int compareBy = interval switch
        {
            BybitInterval.OneMonth => 3,
            BybitInterval.OneWeek => 2,
            _ => 1
        };
        for (int step = 1; step < marketKlines.Count; step++)
        {
            if (compareBy == 1 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddMinutes(intInterval / 60))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 2 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddDays(7))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 3 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddMonths(1))
            {
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
        }
        if (gaps.Count == 0) Console.WriteLine($"\tData is full: {marketKlines.First().OpenTime} - {marketKlines.Last().OpenTime}, {(double)elapsedMilliseconds / 1000} sec, {marketKlines.Count} records - {interval}");
        else Console.WriteLine("Data with gaps");
        return marketKlines;
    }


    public List<ArmedBybitMarketKline> GetSeveralCandles(int n, List<ArmedBybitMarketKline> data, int startIndex)
    {
        List<ArmedBybitMarketKline> finalData = new(Math.Abs(n));
        Func<int, int, bool> predicate = (start, end) => n < 0 ? start > end : start < end;
        for (int i = startIndex; predicate(i, startIndex + n); i += n > 0 ? 1 : -1)
        {
            finalData.Add(data[i]);
        }
        return n < 0 ? finalData.Reverse<ArmedBybitMarketKline>().ToList() : finalData;
    }
}
