using InfluxDB.Client.Core;

namespace Azure.Samples.ReliableEdgeRelay.Models
{
	/// <summary>
	/// The BackfillRequest is the request sent to the device to fill a single data gap
	/// </summary>
	class OpcUaTelemetry
	{
		[Column("BatchId", IsTag = true)]
		public string BatchId { get; set; }

		/// <summary>
		/// Telemetry message
		/// </summary>
		[Column("Telemetry")]
		public string Telemetry { get; set; }

		/// <summary>
		/// Start Window
		/// </summary>
		[Column("StartWindow")]
		public string StartWindow { get; set; }

		/// <summary>
		/// End Window
		/// </summary>
		[Column("EndWindow")]
		public string EndWindow { get; set; }

	}
}