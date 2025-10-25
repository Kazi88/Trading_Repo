using System.Text;
using Bybit.Api.Enums;
using static CandleType;

public class Signaler
{
    public static readonly object _locker = new();
    CandleType Signal;
    public List<(CandleType type, DateTime date)> Signals = new();

    public async Task GetSignalsByQuoteVolume(DbRelation relation, string tokenName, BybitInterval interval, int candlesCount)
    {
        List<ArmedBybitMarketKline> klines = (await relation.GetCandles(tokenName, interval)).TakeLast(10000).ToList();
        decimal bullQV = 0;
        decimal bearQV = 0;

        for (int i = candlesCount; i < klines.Count; i++)
        {
            for (int j = i - candlesCount; j <= i; j++)
            {
                if (klines[j].Type == Bull) bullQV += klines[j].QuoteVolume;
                else if (klines[j].Type == Bear) bearQV += klines[j].QuoteVolume;
            }
            if (bullQV > bearQV && Signal is Bear)// && klines[i].OpenPrice < klines[i].SMA && klines[i].ClosePrice < klines[i].SMA)
            {
                Signals.Add((Bull, klines[i].OpenTime));
                Signal = Bull;
            }
            else if (bearQV > bullQV && Signal is Bull)// && klines[i].OpenPrice > klines[i].SMA && klines[i].ClosePrice < klines[i].SMA)
            {
                Signals.Add((Bear, klines[i].OpenTime));
                Signal = Bear;
            }
            else Signals.Add((Signal, klines[i].OpenTime));
            bullQV = 0;
            bearQV = 0;
        }
    }

    public async Task GetSignalsByVolume(DbRelation relation, string tokenName, BybitInterval interval, int candlesCount)
    {
        List<ArmedBybitMarketKline> klines = (await relation.GetCandles(tokenName, interval)).TakeLast(10000).ToList();
        decimal bull = 0;
        decimal bear = 0;

        for (int i = candlesCount; i < klines.Count; i++)
        {
            for (int j = i - candlesCount; j <= i; j++)
            {
                if (klines[j].Type == Bull) bull += klines[j].Volume;
                else if (klines[j].Type == Bear) bear += klines[j].Volume;
            }
            if (bull > bear && Signal is Bear)// && klines[i].OpenPrice < klines[i].SMA && klines[i].ClosePrice < klines[i].SMA)
            {
                Signals.Add((Bull, klines[i].OpenTime));
                Signal = Bull;
            }
            else if (bear > bull && Signal is Bull)// && klines[i].OpenPrice > klines[i].SMA && klines[i].ClosePrice < klines[i].SMA)
            {
                Signals.Add((Bear, klines[i].OpenTime));
                Signal = Bear;
            }
            else Signals.Add((Signal, klines[i].OpenTime));
            bull = 0;
            bear = 0;
        }
    }

    public static async Task CombineSignals(DbRelation relation, string tokenName, int candlesCount, params BybitInterval[] intervals)
    {
        Signaler[] Signalers = new Signaler[intervals.Length];
        await relation.InitTable(tokenName);
        Task[] tasks = new Task[intervals.Length];
        for (int i = 0; i < intervals.Length; i++)
        {
            int current = i;
            Signalers[current] = new();
            tasks[current] = Task.Run(async () => await Signalers[current].GetSignalsByQuoteVolume(relation, tokenName, intervals[current], candlesCount));
        }
        await Task.WhenAll(tasks);
        Signaler first = Signalers[0];
        List<(CandleType signal, DateTime date)> equalSignals = new();
        int counter;
        foreach (var i in first.Signals)
        {
            counter = 0;
            foreach(var j in Signalers.Skip(1))
            {
                if (j.Signals.Contains(i)) counter++;
                if (counter == Signalers.Length - 1)
                {
                    equalSignals.Add(i);
                    counter = 0;
                }
            }
        }

        CandleType currentSignal = Zero;
        StringBuilder strb = new();

        Console.WriteLine("BEGIN");
        foreach (var i in equalSignals.Take(equalSignals.Count - 10))
        {
            if (i.signal != currentSignal)
            {
                strb.Append($"{i.signal}: {i.date} - ");
                currentSignal = i.signal;
            }
            if (i.signal != equalSignals[equalSignals.IndexOf(i) + 1].signal) strb.Append($"{i.date}\n");
        }
        Console.WriteLine(strb);
        double bull = 0;
        double bear = 0;
        foreach (var i in Signalers)
        {
            if (i.Signal == Bull) bull++;
            else bear++;
        }
        string currentS = bull > bear ? "Bull" : bull < bear ? "Bear" : "50 / 50";
        int probability = (int)((currentS == "Bull" ? bull : currentS == "Bear" ? bear : 2) / (bull + bear) * 100);
        foreach (var Signaler in Signalers)
        {
            Console.WriteLine($"Current: {Signaler.Signal} - {intervals[Array.IndexOf(Signalers, Signaler)]}");
        }
        Console.WriteLine($"CurrentSignal: {currentS} {probability}%");
    }
    
}
