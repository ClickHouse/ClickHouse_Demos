import { useEffect, useMemo, useRef } from "react";
import * as echarts from "echarts";

type Props = {
  option: echarts.EChartsOption;
  height?: number;
};

export function EChart({ option, height = 320 }: Props) {
  const ref = useRef<HTMLDivElement | null>(null);

  const stableOption = useMemo(() => option, [option]);

  useEffect(() => {
    if (!ref.current) return;
    const chart = echarts.init(ref.current, undefined, { renderer: "canvas" });
    chart.setOption(stableOption, { notMerge: true });

    const ro = new ResizeObserver(() => chart.resize());
    ro.observe(ref.current);

    return () => {
      ro.disconnect();
      chart.dispose();
    };
  }, [stableOption]);

  return <div ref={ref} style={{ width: "100%", height }} />;
}

