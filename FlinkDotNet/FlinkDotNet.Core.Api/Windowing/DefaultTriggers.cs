using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing
{
    // --- Concrete Trigger Implementations (Stubs for now) ---

    public class EventTimeTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResults OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx)
        {
            if (window.MaxTimestamp() <= ctx.CurrentWatermark)
            {
                return TriggerResults.Fire; // Fire if watermark already passed window end
            }
            else
            {
                ctx.RegisterEventTimeTimer(window.MaxTimestamp());
                return TriggerResults.None;
            }
        }

        public override TriggerResults OnProcessingTime(long time, TWindow window, ITriggerContext ctx) => TriggerResults.None;

        public override TriggerResults OnEventTime(long time, TWindow window, ITriggerContext ctx)
        {
            return time == window.MaxTimestamp() ? TriggerResults.Fire : TriggerResults.None;
        }
        public override void Clear(TWindow window, ITriggerContext ctx) => ctx.DeleteEventTimeTimer(window.MaxTimestamp());
    }

    public class ProcessingTimeTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResults OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx)
        {
            ctx.RegisterProcessingTimeTimer(window.MaxTimestamp());
            return TriggerResults.None;
        }
        public override TriggerResults OnProcessingTime(long time, TWindow window, ITriggerContext ctx)
        {
            return time == window.MaxTimestamp() ? TriggerResults.Fire : TriggerResults.None;
        }
        public override TriggerResults OnEventTime(long time, TWindow window, ITriggerContext ctx) => TriggerResults.None;
        public override void Clear(TWindow window, ITriggerContext ctx) => ctx.DeleteProcessingTimeTimer(window.MaxTimestamp());
    }

    public class NeverTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResults OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx) => TriggerResults.None;
        public override TriggerResults OnProcessingTime(long time, TWindow window, ITriggerContext ctx) => TriggerResults.None;
        public override TriggerResults OnEventTime(long time, TWindow window, ITriggerContext ctx) => TriggerResults.None;
        public override void Clear(TWindow window, ITriggerContext ctx) { }
    }
}
