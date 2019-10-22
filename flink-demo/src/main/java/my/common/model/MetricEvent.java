package my.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetricEvent {

	/**
	 * Metric name
	 */
	private String id;

	/**
	 * Metric timestamp
	 */
	private Long timestampOp;

	private Long num;

	private String ts;

}
