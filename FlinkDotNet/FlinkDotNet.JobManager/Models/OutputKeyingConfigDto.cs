namespace FlinkDotNet.JobManager.Models
{
    public class OutputKeyingConfigDto
    {
        /// <summary>
        /// String representation of the key selector logic.
        /// Examples: "prop:OrderId", "field:customerCategory", "type:MyProject.MyKeySelector, MyAssembly"
        /// This string will be parsed by the TaskManager to activate the key selection.
        /// </summary>
        public string? SerializedKeySelector { get; set; }

        /// <summary>
        /// Assembly-qualified name of the key's data type.
        /// Example: "System.String", "System.Int32", "MyProject.Types.OrderKey, MyAssembly"
        /// </summary>
        public string? KeyTypeName { get; set; }
    }
}
#nullable disable
