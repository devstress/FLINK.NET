using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Microsoft.Fast.Components.FluentUI;

var builder = WebAssemblyHostBuilder.CreateDefault(args);

using FlinkDotNet.WebUI.Services; // Add this for ThemeService

builder.Services.AddFluentUI();
builder.Services.AddScoped(sp => new HttpClient { BaseAddress = new Uri(builder.HostEnvironment.BaseAddress) });
builder.Services.AddScoped<ThemeService>(); // Register ThemeService

var host = builder.Build(); // Build the host first

// Initialize ThemeService after building the host, to ensure JS interop is available
var themeService = host.Services.GetRequiredService<ThemeService>();
await themeService.InitializeAsync();

await host.RunAsync();
