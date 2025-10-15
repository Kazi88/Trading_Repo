public class Program
{
    public static async Task Main()
    {
        Parser parser = Parser.GetParser("key", "keySecret");
        DbRelation relation = new(parser);
        await relation.InitTables();
    }
}