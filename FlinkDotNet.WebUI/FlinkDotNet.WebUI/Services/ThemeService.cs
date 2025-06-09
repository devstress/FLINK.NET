using System;
using System.Threading.Tasks;
using Microsoft.FluentUI.AspNetCore.Components; // For StandardLuminance
using Microsoft.JSInterop; // Required for IJSRuntime

namespace FlinkDotNet.WebUI.Services
{
    public class ThemeService
    {
        private readonly IJSRuntime _jsRuntime;
        private const string ThemeStorageKey = "userPreferredTheme";

        public StandardLuminance CurrentLuminance { get; private set; } = StandardLuminance.LightMode; // Default to LightMode

        public event Action? OnThemeChanged;

        public ThemeService(IJSRuntime jsRuntime)
        {
            _jsRuntime = jsRuntime;
        }

        public async Task InitializeAsync()
        {
            try
            {
                var storedTheme = await _jsRuntime.InvokeAsync<string?>("localStorage.getItem", ThemeStorageKey);
                if (!string.IsNullOrEmpty(storedTheme))
                {
                    CurrentLuminance = storedTheme == "dark" ? StandardLuminance.DarkMode : StandardLuminance.LightMode;
                }
                else
                {
                    // Optional: could check system preference here if FluentThemeProvider doesn't do it
                    CurrentLuminance = StandardLuminance.LightMode; // Default if nothing stored
                }
            }
            catch (Exception ex)
            {
                // LocalStorage might not be available (e.g., during prerendering or if disabled)
                Console.WriteLine($"Error initializing theme from localStorage: {ex.Message}");
                CurrentLuminance = StandardLuminance.LightMode; // Fallback to default
            }
            NotifyThemeChanged();
        }

        public async Task ToggleThemeAsync()
        {
            CurrentLuminance = CurrentLuminance == StandardLuminance.LightMode ? StandardLuminance.DarkMode : StandardLuminance.LightMode;
            try
            {
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", ThemeStorageKey, CurrentLuminance == StandardLuminance.DarkMode ? "dark" : "light");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving theme to localStorage: {ex.Message}");
            }
            NotifyThemeChanged();
        }

        public async Task SetLuminanceAsync(StandardLuminance luminance)
        {
            CurrentLuminance = luminance;
            try
            {
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", ThemeStorageKey, CurrentLuminance == StandardLuminance.DarkMode ? "dark" : "light");
            }
            catch (Exception ex)
            {
                 Console.WriteLine($"Error saving theme to localStorage: {ex.Message}");
            }
            NotifyThemeChanged();
        }

        private void NotifyThemeChanged()
        {
            OnThemeChanged?.Invoke();
        }
    }
}
