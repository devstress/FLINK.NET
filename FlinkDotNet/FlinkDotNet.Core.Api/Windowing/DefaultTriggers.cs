using FlinkDotNet.Core.Abstractions.Windowing; // For Window

namespace FlinkDotNet.Core.Api.Windowing
{
    // --- Concrete Trigger Implementations (Stubs for now) ---

    public class EventTimeTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResult OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx)
        {
            if (window.MaxTimestamp() <= ctx.CurrentWatermark)
            {
                return TriggerResult.Fire; // Fire if watermark already passed window end
            }
            else
            {
                ctx.RegisterEventTimeTimer(window.MaxTimestamp());
                return TriggerResult.None;
            }
        }

        public override TriggerResult OnProcessingTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.None;

        public override TriggerResult OnEventTime(long time, TWindow window, ITriggerContext ctx)
        {
            return time == window.MaxTimestamp() ? TriggerResult.Fire : TriggerResult.None;
        }
        public override void Clear(TWindow window, ITriggerContext ctx) => ctx.DeleteEventTimeTimer(window.MaxTimestamp());
    }

    public class ProcessingTimeTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResult OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx)
        {
            ctx.RegisterProcessingTimeTimer(window.MaxTimestamp());
            return TriggerResult.None;
        }
        public override TriggerResult OnProcessingTime(long time, TWindow window, ITriggerContext ctx)
        {
            return time == window.MaxTimestamp() ? TriggerResult.Fire : TriggerResult.None;
        }
        public override TriggerResult OnEventTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.None;
        public override void Clear(TWindow window, ITriggerContext ctx) => ctx.DeleteProcessingTimeTimer(window.MaxTimestamp());
    }

    public class NeverTrigger<TElement, TWindow> : Trigger<TElement, TWindow> where TWindow : Window
    {
        public override TriggerResult OnElement(TElement element, long timestamp, TWindow window, ITriggerContext ctx) => TriggerResult.None;
        public override TriggerResult OnProcessingTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.None;
        public override TriggerResult OnEventTime(long time, TWindow window, ITriggerContext ctx) => TriggerResult.None;
        public override void Clear(TWindow window, ITriggerContext ctx) { }
    }
}
