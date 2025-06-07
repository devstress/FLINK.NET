using System;
using System.Threading.Tasks;
using Microsoft.Fast.Components.FluentUI.DesignTokens; // Required for Luminance
using Microsoft.JSInterop; // Required for IJSRuntime

namespace FlinkDotNet.WebUI.Services
{
    public class ThemeService
    {
        private readonly IJSRuntime _jsRuntime;
        private const string ThemeStorageKey = "userPreferredTheme";

        public BaseLayerLuminance CurrentLuminance { get; private set; } = BaseLayerLuminance.Light; // Default to Light

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
                    CurrentLuminance = storedTheme == "dark" ? BaseLayerLuminance.Dark : BaseLayerLuminance.Light;
                }
                else
                {
                    // Optional: could check system preference here if FluentThemeProvider doesn't do it
                    CurrentLuminance = BaseLayerLuminance.Light; // Default if nothing stored
                }
            }
            catch (Exception ex)
            {
                // LocalStorage might not be available (e.g., during prerendering or if disabled)
                Console.WriteLine($"Error initializing theme from localStorage: {ex.Message}");
                CurrentLuminance = BaseLayerLuminance.Light; // Fallback to default
            }
            NotifyThemeChanged();
        }

        public async Task ToggleThemeAsync()
        {
            CurrentLuminance = CurrentLuminance == BaseLayerLuminance.Light ? BaseLayerLuminance.Dark : BaseLayerLuminance.Light;
            try
            {
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", ThemeStorageKey, CurrentLuminance == BaseLayerLuminance.Dark ? "dark" : "light");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error saving theme to localStorage: {ex.Message}");
            }
            NotifyThemeChanged();
        }

        public async Task SetLuminanceAsync(BaseLayerLuminance luminance)
        {
            CurrentLuminance = luminance;
            try
            {
                await _jsRuntime.InvokeVoidAsync("localStorage.setItem", ThemeStorageKey, CurrentLuminance == BaseLayerLuminance.Dark ? "dark" : "light");
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
#nullable disable
