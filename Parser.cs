using System.Diagnostics;
using ApiSharp.Authentication;
using ApiSharp.Extensions;
using Bybit.Api;
using Bybit.Api.Enums;
using Bybit.Api.Market;
using Bybit.Net.Clients;
using Bybit.Net.Enums;


public class Parser
{
    private static Parser _parser = null!;
    private static BybitRestApiClient _apiClient = null!;
    private static BybitRestClient _restClient = null!;
    private readonly object _locker = new();
    public List<string> futuresList = new();
    private Parser(string keyApi, string apiSecret, bool isTestNet)
    {
        BybitRestApiClientOptions apiOptions = new(new ApiCredentials(keyApi, apiSecret))
        {
            BaseAddress = isTestNet ? BybitAddress.TestNet.RestApiAddress : BybitAddress.MainNet.RestApiAddress,
        };
        _apiClient = new(apiOptions);
        _restClient = new();
    }

    public static Parser GetParser(string keyApi, string apiSecret, bool isTestNet = false)
    {
        if (_parser is null) return new Parser(keyApi, apiSecret, isTestNet);
        else return _parser;
    }

    public async Task GetFuturesNamesAsync(string? cursor = null)
    {
        var request = await _restClient.V5Api.ExchangeData.GetLinearInverseSymbolsAsync(Category.Linear, cursor: cursor);
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
        List<BybitMarketKline> marketKlines = new();
        DateTime launchDate = (await _apiClient.Market.GetLinearInstrumentsAsync(currencyName)).Data.First().LaunchTime.AddDays(-1).ToUniversalTime();
        ChartRequest request = new(currencyName, interval, launchDate, DateTime.UtcNow);
        return await GetChartForTerm(request, marketKlines);
    }

    public async Task<List<BybitMarketKline>> GetChartForTerm(string currencyName, BybitInterval interval, DateTime start, DateTime end)
    {
        List<BybitMarketKline> marketKlines = new();
        return await GetChartForTerm(new ChartRequest(currencyName, interval, start, end), marketKlines);
    }

    private async Task<List<BybitMarketKline>> GetChartForTerm(ChartRequest request, List<BybitMarketKline> marketKlines)
    {
        if (request.TasksCount == 0) // interval - week or month OR small timeframe
        {
            try
            {
                Console.Write($"|{request.CurrencyName} - {request.IntervalToSearch}|");
                var query = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, request.CurrencyName, request.IntervalToSearch, request.StartPoint, request.EndPoint, 1000);
                foreach (var kline in query.Data) marketKlines.Add(kline);
                return SortAndCheck(marketKlines, request.IntervalToSearch, out _);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.Message} GetChartForTerm ({request.IntervalToSearch}) {request.StartPoint} - {request.EndPoint}");
                return [];
            }
        }
        Task[] finders = new Task[request.TasksCount];
        Stopwatch sw = new();
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
                        query = await _apiClient.Market.GetKlinesAsync(BybitCategory.Linear, request.CurrencyName, request.IntervalToSearch, localStartPoint, localEndPoint, 1000);
                    }
                });
            }
            await Task.WhenAll(finders);
            sw.Stop();
            return SortAndCheck(marketKlines, request.IntervalToSearch, out _, sw.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            return [];
        }
    }

    public static List<BybitMarketKline> SortAndCheck(List<BybitMarketKline> marketKlines, BybitInterval interval, out List<(DateTime, DateTime)> gaps, long elapsedMilliseconds = 1000)
    {
        gaps = new();
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
                // Console.WriteLine($"\tError: Gaps found:: {marketKlines[step - 1].OpenTime} - {marketKlines[step].OpenTime}, {interval}");
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 2 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddDays(7))
            {
                // Console.WriteLine($"\tError: Gaps found:: {marketKlines[step - 1].OpenTime} - {marketKlines[step].OpenTime}, {interval}");
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
            else if (compareBy == 3 && marketKlines[step].OpenTime != marketKlines[step - 1].OpenTime.AddMonths(1))
            {
                // Console.WriteLine($"\tError: Gaps found:: {marketKlines[step - 1].OpenTime} - {marketKlines[step].OpenTime}, {interval}");
                gaps.Add((marketKlines[step - 1].OpenTime, marketKlines[step].OpenTime));
                continue;
            }
        }
        if (gaps.Count == 0) Console.WriteLine($"\tData is full: {marketKlines.First().OpenTime} - {marketKlines.Last().OpenTime}, {(double)elapsedMilliseconds / 1000} sec, {marketKlines.Count} records - {interval}");
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

public class ChartRequest : ForceRequest<DateTime>
{
    public string? CurrencyName { get; set; }
    public double IntervalToSkip { get; set; }
    public BybitInterval IntervalToSearch{ get; set; }
    public ChartRequest(string currencyName, BybitInterval intervalToSearch, DateTime startPoint, DateTime endPoint)
    : base(startPoint, endPoint)
    {
        CurrencyName = currencyName;
        IntervalToSearch = intervalToSearch;
        SetForceOptions();
    }
    protected override void SetForceOptions()
    {
        long totalTimeFrame = EndPoint.ConvertToSeconds() - StartPoint.ConvertToSeconds();
        long records = totalTimeFrame / 60 / ((int)IntervalToSearch / 60);
        if (records <= 1000 || totalTimeFrame <= 1000 || IntervalToSearch == BybitInterval.OneWeek || IntervalToSearch == BybitInterval.OneMonth) return;
        IntervalToSkip = totalTimeFrame / Environment.ProcessorCount;
        TasksCount = Math.Min((int)Math.Ceiling((double)records / 1000), Environment.ProcessorCount);
        FunctionsStartPoints = new DateTime[TasksCount];
        for (int step = 0; step < TasksCount; step++)
        {
            FunctionsStartPoints[step] = step == 0 ? StartPoint : (StartPoint.ConvertToSeconds() + IntervalToSkip * step).ConvertFromSeconds().ToUniversalTime();
        }
    }
}

public abstract class ForceRequest<P>
{
    public P StartPoint { get; set; }
    public P EndPoint { get; set; }
    public P[] FunctionsStartPoints { get; set; } = [];
    public int TasksCount;
    public ForceRequest(P startPoint, P endPoint)
    {
        StartPoint = startPoint;
        EndPoint = endPoint;
    }

    protected abstract void SetForceOptions();
}