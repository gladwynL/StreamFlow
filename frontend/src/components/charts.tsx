import type { ReactNode } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { SERIES } from './chartColors';

const AXIS = {
  stroke: 'var(--color-line-strong)',
  tick: { fill: 'var(--color-ink-faint)', fontSize: 11 },
};

export interface SeriesDef<T extends object> {
  key: keyof T & string;
  label: string;
}

/**
 * A chart with its title, the question it answers, a legend (for two or more series) and a data table for
 * screen readers and exact values.
 */
export function ChartFrame({
  title,
  description,
  legend,
  table,
  children,
}: {
  title: string;
  description: string;
  legend?: { label: string; color: string; dashed?: boolean }[];
  table: ReactNode;
  children: ReactNode;
}) {
  return (
    <figure className="rounded-xl border border-line bg-panel p-5">
      <figcaption className="mb-3">
        <p className="text-sm font-semibold text-ink">{title}</p>
        <p className="mt-0.5 text-xs text-ink-muted">{description}</p>
        {legend && legend.length > 1 && (
          <ul className="mt-2 flex flex-wrap gap-x-4 gap-y-1" aria-label="Legend">
            {legend.map((item) => (
              <li key={item.label} className="flex items-center gap-1.5 text-xs text-ink-muted">
                <svg aria-hidden="true" width="16" height="8">
                  <line
                    x1="0"
                    y1="4"
                    x2="16"
                    y2="4"
                    stroke={item.color}
                    strokeWidth="2"
                    strokeDasharray={item.dashed ? '4 3' : undefined}
                  />
                </svg>
                {item.label}
              </li>
            ))}
          </ul>
        )}
      </figcaption>
      {/* Charts are hidden from assistive technology and not focusable; the data table below carries the
          same values for screen-reader and keyboard users. */}
      <div aria-hidden="true">{children}</div>
      <details className="mt-3 text-xs text-ink-muted">
        <summary className="cursor-pointer select-none hover:text-ink">Show data table</summary>
        <div className="mt-2 max-h-64 overflow-auto">{table}</div>
      </details>
    </figure>
  );
}

function TooltipBox({
  active,
  label,
  payload,
  labelFormat,
  valueFormat,
}: {
  active?: boolean;
  label?: string | number;
  payload?: readonly { name?: string | number; value?: unknown; color?: string }[];
  labelFormat: (label: string) => string;
  valueFormat: (value: number) => string;
}) {
  if (!active || !payload || payload.length === 0) {
    return null;
  }
  return (
    <div className="rounded-md border border-line-strong bg-panel-raised px-3 py-2 text-xs shadow-lg">
      <p className="mb-1 text-ink-muted">{labelFormat(String(label))}</p>
      {payload.map((entry) => (
        <p key={String(entry.name)} className="flex items-center gap-2 text-ink">
          <span
            aria-hidden="true"
            className="inline-block h-0.5 w-3"
            style={{ background: entry.color }}
          />
          <span className="text-ink-muted">{entry.name}</span>
          <span className="tabular ml-auto font-mono">
            {typeof entry.value === 'number' ? valueFormat(entry.value) : '—'}
          </span>
        </p>
      ))}
    </div>
  );
}

/** Lines over time; one y-axis (all series share a unit). Nulls are gaps, not zeros. */
export function TimeLines<T extends object>({
  data,
  xKey,
  series,
  xFormat,
  yFormat,
  height = 220,
  dashedFrom,
}: {
  data: T[];
  xKey: keyof T & string;
  series: SeriesDef<T>[];
  xFormat: (value: string) => string;
  yFormat: (value: number) => string;
  height?: number;
  /** Series at or after this index are dashed (secondary encoding besides colour). */
  dashedFrom?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <LineChart
        data={data}
        margin={{ top: 8, right: 12, bottom: 0, left: 0 }}
        accessibilityLayer={false}
      >
        <CartesianGrid stroke="var(--color-line)" strokeDasharray="2 4" vertical={false} />
        <XAxis
          dataKey={String(xKey)}
          tickFormatter={xFormat}
          {...AXIS}
          minTickGap={40}
          tickLine={false}
          axisLine={false}
        />
        <YAxis tickFormatter={yFormat} {...AXIS} width={56} tickLine={false} axisLine={false} />
        <Tooltip
          cursor={{ stroke: 'var(--color-ink-faint)', strokeWidth: 1 }}
          content={(props) => <TooltipBox {...props} labelFormat={xFormat} valueFormat={yFormat} />}
        />
        {series.map((s, index) => (
          <Line
            key={s.key}
            type="monotone"
            dataKey={String(s.key)}
            name={s.label}
            stroke={SERIES[index % SERIES.length]}
            strokeWidth={2}
            strokeDasharray={dashedFrom !== undefined && index >= dashedFrom ? '5 4' : undefined}
            dot={false}
            activeDot={{ r: 4, strokeWidth: 2, stroke: 'var(--color-panel)' }}
            connectNulls={false}
            isAnimationActive={false}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

/** Bars for one measure across categories (magnitude, so a single hue). */
export function CategoryBars<T extends object>({
  data,
  categoryKey,
  valueKey,
  valueLabel,
  categoryFormat,
  valueFormat,
  height = 240,
}: {
  data: T[];
  categoryKey: keyof T & string;
  valueKey: keyof T & string;
  valueLabel: string;
  categoryFormat: (value: string) => string;
  valueFormat: (value: number) => string;
  height?: number;
}) {
  return (
    <ResponsiveContainer width="100%" height={height}>
      <BarChart
        data={data}
        layout="vertical"
        margin={{ top: 4, right: 16, bottom: 0, left: 8 }}
        accessibilityLayer={false}
      >
        <CartesianGrid stroke="var(--color-line)" strokeDasharray="2 4" horizontal={false} />
        <XAxis
          type="number"
          tickFormatter={valueFormat}
          {...AXIS}
          tickLine={false}
          axisLine={false}
        />
        <YAxis
          type="category"
          dataKey={String(categoryKey)}
          tickFormatter={categoryFormat}
          {...AXIS}
          width={130}
          tickLine={false}
          axisLine={false}
        />
        <Tooltip
          cursor={{ fill: 'rgb(255 255 255 / 0.04)' }}
          content={(props) => (
            <TooltipBox {...props} labelFormat={categoryFormat} valueFormat={valueFormat} />
          )}
        />
        <Bar
          dataKey={String(valueKey)}
          name={valueLabel}
          fill={SERIES[0]}
          radius={[0, 4, 4, 0]}
          barSize={14}
          isAnimationActive={false}
        />
      </BarChart>
    </ResponsiveContainer>
  );
}

/** A compact accessible data table for chart frames. */
export function DataTable({ head, rows }: { head: string[]; rows: (string | number | null)[][] }) {
  return (
    <table className="tabular w-full text-left font-mono">
      <thead>
        <tr>
          {head.map((h) => (
            <th
              key={h}
              scope="col"
              className="sticky top-0 bg-panel py-1 pr-3 font-sans font-medium text-ink-muted"
            >
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i} className="border-t border-line/60">
            {row.map((cell, j) => (
              <td key={j} className="py-1 pr-3 text-ink">
                {cell ?? '—'}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
