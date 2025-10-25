using ApiSharp.Extensions;
using Bybit.Api.Enums;

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

public class ChartRequest : ForceRequest<DateTime>
{
    public string CurrencyName { get; set; }
    public double IntervalToSkip { get; set; }
    public BybitInterval IntervalToSearch { get; set; }
    public int Capacity{ get; set; }
    public ChartRequest(string currencyName, BybitInterval intervalToSearch, DateTime startPoint, DateTime endPoint)
    : base(startPoint, endPoint)
    {
        Capacity = (int)((endPoint - startPoint).TotalMinutes / ((int)intervalToSearch / 60));
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
