using System;
using System.Reflection;
using System.Threading.Tasks;

namespace IntegrationTestImage;

public class Program
{
    public static async Task Main(string[] args)
    {
        var assembly = Assembly.Load("FlinkDotNetAspire.AppHost");
        var program = assembly.GetType("Program") ?? assembly.GetType("FlinkDotNetAspire.AppHost.Program");
        var main = program?.GetMethod("Main", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);
        if (main == null)
        {
            throw new MissingMethodException("Unable to locate AppHost entry point");
        }
        var result = main.Invoke(null, new object[] { args });
        if (result is Task task)
        {
            await task.ConfigureAwait(false);
        }
    }
}
