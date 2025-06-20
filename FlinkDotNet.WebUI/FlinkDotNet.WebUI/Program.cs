using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Microsoft.FluentUI.AspNetCore.Components; // Required for AddFluentUIComponents
using FlinkDotNet.WebUI.Services; // Add this for ThemeService

var builder = WebAssemblyHostBuilder.CreateDefault(args);

builder.Services.AddFluentUIComponents(); // Changed to AddFluentUIComponents
builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });
builder.Services.AddScoped<ThemeService>(); // Register ThemeService

var host = builder.Build(); // Build the host first

// Initialize ThemeService after building the host, to ensure JS interop is available
var themeService = host.Services.GetRequiredService<ThemeService>();
await themeService.InitializeAsync();

await host.RunAsync();
