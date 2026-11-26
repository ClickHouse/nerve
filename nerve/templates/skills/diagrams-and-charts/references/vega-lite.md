# Vega-lite for the Nerve chat

The web chat renders vega-lite 6 (fence `vega-lite` or `vegalite`) and vega 6
(fence `vega`). Use vega-lite: it is shorter and the chat's defaults fit it.
Use full vega only for a layout vega-lite cannot express.

## The skeleton

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Plan B costs a third less",
  "description": "Monthly cost of three plans: A 900, B 600, C 750.",
  "data": {"values": [
    {"plan": "A", "cost": 900}, {"plan": "B", "cost": 600}, {"plan": "C", "cost": 750}
  ]},
  "mark": "bar",
  "encoding": {
    "x": {"field": "plan", "type": "nominal", "title": null},
    "y": {"field": "cost", "type": "quantitative", "title": "Cost per month"}
  }
}
```

- `title` states the finding; `description` says what the chart shows, with
  the range of the data, for screen readers.
- `data.values` only. A `url` makes the chat refuse the chart.
- Every field has a `type`. No `width`. No colours, fonts or `config` unless a
  colour means something.

## What the chat sets for you

- Width: a single view fills the column (`width: "container"`). A view with
  `concat`, `facet` or `repeat` cannot: give each view its own `width`.
- Height: 280 px when the spec gives none. A y axis of categories gets about
  20 px a category. A pie or donut takes a 280 px square. `"height":
  "container"` counts as no height.
- Look: the app's palette (blue for one series), its font, 11 px labels, a
  14 px title on the left, light gridlines, no box around the plot. A value
  axis has no axis line and about 5 ticks; a time axis has no grid.
- Legends sit on the right; on a column narrower than 480 px they go on top.
- Bars have small rounded ends and a hairline gap. Areas are a light wash under
  a line.
- Sequential colour scales run from the surface to the text colour in one hue;
  diverging scales run from orange through grey to blue.
- Category labels on the x axis lie flat while they fit (when the categories
  are in `data.values`); an angle you set stays.
- Dates: date-only strings ("2027-03-08") in a `temporal` field are read as
  local dates, so they show as written. Each tick of a time axis reads like
  "Mar 08"; the first tick of a year reads "2027". A field with a `timeUnit`, or
  an axis with its own `format`, keeps its own labels.
- Tooltips work. A field called `image` shows as text.
- `usermeta` is ignored; there is no action menu. `$schema` decides between
  vega and vega-lite when the fence says the other.
- Downloads: SVG and PNG, drawn over the chart's background.

## Recipes

Values by category, sorted, labelled at the bar end (no axis to read):

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Email brings most tickets",
  "description": "Support tickets in March by channel: email 412, chat 260, phone 133, forum 58.",
  "data": {"values": [
    {"channel": "Email", "tickets": 412}, {"channel": "Chat", "tickets": 260},
    {"channel": "Phone", "tickets": 133}, {"channel": "Forum", "tickets": 58}
  ]},
  "encoding": {
    "y": {"field": "channel", "type": "nominal", "sort": {"op": "sum", "field": "tickets", "order": "descending"}, "title": null},
    "x": {"field": "tickets", "type": "quantitative", "title": "Tickets", "axis": null}
  },
  "layer": [
    {"mark": "bar"},
    {"mark": {"type": "text", "align": "left", "dx": 4}, "encoding": {"text": {"field": "tickets", "type": "quantitative"}}}
  ]
}
```

Bars over dates. A `bar` on a `temporal` x is one day wide, so put the dates
on an `ordinal` axis. Use a `utc` time unit: a local one shows a day early west
of Greenwich.

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Deploys dropped in the release freeze",
  "description": "Deploys per week, January to February 2027: about 20 a week, 4 in the freeze week of February 8.",
  "data": {"values": [
    {"week": "2027-01-04", "deploys": 21}, {"week": "2027-01-11", "deploys": 19},
    {"week": "2027-01-18", "deploys": 23}, {"week": "2027-01-25", "deploys": 20},
    {"week": "2027-02-01", "deploys": 18}, {"week": "2027-02-08", "deploys": 4},
    {"week": "2027-02-15", "deploys": 22}
  ]},
  "mark": "bar",
  "encoding": {
    "x": {"field": "week", "type": "ordinal", "timeUnit": "utcyearmonthdate", "title": "Week starting",
          "axis": {"format": "%b %d"}},
    "y": {"field": "deploys", "type": "quantitative", "title": "Deploys"}
  }
}
```

A trend with two series, each labelled at its last point instead of a legend:

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Mobile passed desktop in May",
  "description": "Monthly active users by platform, January to June 2027. Desktop falls from 5200 to 4700, mobile rises from 3900 to 5600.",
  "data": {"values": [
    {"month": "2027-01-01", "platform": "Desktop", "users": 5200}, {"month": "2027-01-01", "platform": "Mobile", "users": 3900},
    {"month": "2027-02-01", "platform": "Desktop", "users": 5100}, {"month": "2027-02-01", "platform": "Mobile", "users": 4200},
    {"month": "2027-03-01", "platform": "Desktop", "users": 5000}, {"month": "2027-03-01", "platform": "Mobile", "users": 4600},
    {"month": "2027-04-01", "platform": "Desktop", "users": 4950}, {"month": "2027-04-01", "platform": "Mobile", "users": 4900},
    {"month": "2027-05-01", "platform": "Desktop", "users": 4800}, {"month": "2027-05-01", "platform": "Mobile", "users": 5300},
    {"month": "2027-06-01", "platform": "Desktop", "users": 4700}, {"month": "2027-06-01", "platform": "Mobile", "users": 5600}
  ]},
  "encoding": {
    "x": {"field": "month", "type": "temporal", "title": null, "axis": {"tickCount": "month"}},
    "y": {"field": "users", "type": "quantitative", "title": "Active users"},
    "color": {"field": "platform", "type": "nominal", "legend": null}
  },
  "layer": [
    {"mark": "line"},
    {"mark": {"type": "text", "align": "left", "dx": 6},
     "encoding": {
       "x": {"aggregate": "max", "field": "month", "type": "temporal"},
       "y": {"aggregate": {"argmax": "month"}, "field": "users", "type": "quantitative"},
       "text": {"field": "platform", "type": "nominal"}}}
  ]
}
```

For daily data use `"axis": {"tickCount": "day"}` (or `"week"`), so ticks fall
on whole days and not on "12:00".

A value against a target (a layer of a line, a rule and a label):

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "p95 latency stays under the 300 ms target",
  "description": "Daily p95 latency for one week, between 210 and 280 ms, with a rule at the 300 ms target.",
  "data": {"values": [
    {"day": "2027-01-04", "p95": 230}, {"day": "2027-01-05", "p95": 245}, {"day": "2027-01-06", "p95": 280},
    {"day": "2027-01-07", "p95": 262}, {"day": "2027-01-08", "p95": 240}, {"day": "2027-01-09", "p95": 215},
    {"day": "2027-01-10", "p95": 210}
  ]},
  "layer": [
    {"mark": {"type": "line", "point": true},
     "encoding": {
       "x": {"field": "day", "type": "temporal", "title": null, "axis": {"tickCount": "day"}},
       "y": {"field": "p95", "type": "quantitative", "title": "p95 (ms)", "scale": {"domain": [0, 320], "nice": false}}}},
    {"mark": {"type": "rule", "strokeDash": [4, 4]}, "encoding": {"y": {"datum": 300}}},
    {"mark": {"type": "text", "align": "left", "dx": 4, "dy": -8},
     "encoding": {"x": {"datum": "2027-01-04", "type": "temporal"}, "y": {"datum": 300}, "text": {"value": "target 300 ms"}}}
  ]
}
```

Part of a whole across groups (100 % stacked bars). Put the part the title talks
about at the baseline, and the legend in the same order:

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Team B spends half its time on support",
  "description": "Share of hours by kind of work for three teams. Support: team A 20 %, team B 50 %, team C 30 %.",
  "data": {"values": [
    {"team": "A", "kind": "Features", "hours": 60}, {"team": "A", "kind": "Support", "hours": 20}, {"team": "A", "kind": "Maintenance", "hours": 20},
    {"team": "B", "kind": "Features", "hours": 30}, {"team": "B", "kind": "Support", "hours": 50}, {"team": "B", "kind": "Maintenance", "hours": 20},
    {"team": "C", "kind": "Features", "hours": 45}, {"team": "C", "kind": "Support", "hours": 30}, {"team": "C", "kind": "Maintenance", "hours": 25}
  ]},
  "transform": [{"calculate": "indexof(['Support', 'Features', 'Maintenance'], datum.kind)", "as": "rank"}],
  "mark": "bar",
  "encoding": {
    "y": {"field": "team", "type": "nominal", "title": "Team"},
    "x": {"field": "hours", "type": "quantitative", "stack": "normalize", "title": "Share of hours", "axis": {"format": "%"}},
    "color": {"field": "kind", "type": "nominal", "title": null, "sort": ["Support", "Features", "Maintenance"]},
    "order": {"field": "rank", "type": "quantitative"}
  }
}
```

A donut, only for 2 to 5 parts:

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Most visitors come from search",
  "description": "Traffic sources: search 58 %, direct 27 %, social 15 %.",
  "data": {"values": [
    {"source": "Search", "share": 58}, {"source": "Direct", "share": 27}, {"source": "Social", "share": 15}
  ]},
  "mark": {"type": "arc", "innerRadius": 60},
  "encoding": {
    "theta": {"field": "share", "type": "quantitative"},
    "color": {"field": "source", "type": "nominal", "title": null, "sort": ["Search", "Direct", "Social"]},
    "tooltip": [{"field": "source", "type": "nominal"}, {"field": "share", "type": "quantitative", "title": "Share (%)"}]
  }
}
```

A distribution (histogram):

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Most requests finish under 200 ms",
  "description": "Histogram of 20 request durations from 40 to 610 ms.",
  "data": {"values": [
    {"ms": 40}, {"ms": 55}, {"ms": 70}, {"ms": 80}, {"ms": 95}, {"ms": 110}, {"ms": 120}, {"ms": 125},
    {"ms": 130}, {"ms": 140}, {"ms": 150}, {"ms": 160}, {"ms": 170}, {"ms": 185}, {"ms": 190}, {"ms": 240},
    {"ms": 260}, {"ms": 320}, {"ms": 450}, {"ms": 610}
  ]},
  "mark": "bar",
  "encoding": {
    "x": {"field": "ms", "type": "quantitative", "bin": {"step": 50}, "title": "Duration (ms)"},
    "y": {"aggregate": "count", "type": "quantitative", "title": "Requests"}
  }
}
```

For groups, `"mark": "boxplot"` with a nominal x and a quantitative y compares
the spread of each group.

A correlation (scatter), with the item of interest named:

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Bigger teams ship more, except team F",
  "description": "Team size against features shipped for six teams; team F has 9 people and shipped 4 features.",
  "data": {"values": [
    {"team": "A", "size": 3, "shipped": 5}, {"team": "B", "size": 4, "shipped": 7}, {"team": "C", "size": 5, "shipped": 8},
    {"team": "D", "size": 7, "shipped": 11}, {"team": "E", "size": 8, "shipped": 13}, {"team": "F", "size": 9, "shipped": 4}
  ]},
  "encoding": {
    "x": {"field": "size", "type": "quantitative", "title": "People"},
    "y": {"field": "shipped", "type": "quantitative", "title": "Features shipped"}
  },
  "layer": [
    {"mark": "point"},
    {"transform": [{"filter": "datum.team == 'F'"}],
     "mark": {"type": "text", "align": "left", "dx": 8}, "encoding": {"text": {"value": "team F"}}}
  ]
}
```

A matrix (heatmap). Do not write values on the cells in the chat: its colour
ramp runs from light to dark in the light theme and from dark to bright in the
dark theme, so no one text colour reads on every cell in both. Give the values
in a tooltip, or in a table under the chart.

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Errors peak on Monday mornings",
  "description": "Error count by weekday and part of day; the highest is Monday morning with 42.",
  "data": {"values": [
    {"day": "Mon", "part": "Morning", "errors": 42}, {"day": "Mon", "part": "Afternoon", "errors": 18},
    {"day": "Tue", "part": "Morning", "errors": 20}, {"day": "Tue", "part": "Afternoon", "errors": 15},
    {"day": "Wed", "part": "Morning", "errors": 17}, {"day": "Wed", "part": "Afternoon", "errors": 12}
  ]},
  "height": {"step": 36},
  "mark": "rect",
  "encoding": {
    "x": {"field": "day", "type": "ordinal", "sort": ["Mon", "Tue", "Wed"], "title": null, "axis": {"labelAngle": 0}},
    "y": {"field": "part", "type": "ordinal", "sort": ["Morning", "Afternoon"], "title": null},
    "color": {"field": "errors", "type": "quantitative", "title": "Errors"},
    "tooltip": [{"field": "day", "type": "ordinal"}, {"field": "part", "type": "ordinal"}, {"field": "errors", "type": "quantitative"}]
  }
}
```

Point out one bar without a fixed colour (it reads in both themes):

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Acme has the shortest wait",
  "description": "Median wait in minutes at four sites: Acme 6, North 11, East 14, West 17.",
  "data": {"values": [
    {"site": "Acme", "wait": 6}, {"site": "North", "wait": 11}, {"site": "East", "wait": 14}, {"site": "West", "wait": 17}
  ]},
  "mark": "bar",
  "encoding": {
    "x": {"field": "site", "type": "nominal", "sort": "y", "title": null},
    "y": {"field": "wait", "type": "quantitative", "title": "Median wait (min)"},
    "opacity": {"condition": {"test": "datum.site == 'Acme'", "value": 1}, "value": 0.35}
  }
}
```

Small multiples (a `facet` cannot fill the column, so each view has a width):

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v6.json",
  "title": "Every region grew in Q2",
  "description": "Revenue by quarter for two regions; both are higher in Q2 than in Q1.",
  "data": {"values": [
    {"region": "North", "quarter": "Q1", "revenue": 120}, {"region": "North", "quarter": "Q2", "revenue": 150},
    {"region": "South", "quarter": "Q1", "revenue": 90}, {"region": "South", "quarter": "Q2", "revenue": 130}
  ]},
  "facet": {"column": {"field": "region", "type": "nominal", "title": null}},
  "spec": {
    "width": 220,
    "mark": "bar",
    "encoding": {
      "x": {"field": "quarter", "type": "ordinal", "title": null},
      "y": {"field": "revenue", "type": "quantitative", "title": "Revenue (k)"}
    }
  }
}
```

## Common faults

| Symptom | Cause | Fix |
|---|---|---|
| bars only a few pixels wide | `bar` on a `temporal` x | `ordinal` x with a `utc...` time unit |
| a date one day early | local `timeUnit` on a UTC date string | `utcyearmonthdate` or another `utc` time unit |
| ticks at "12:00" between days | too few days for the tick count | `"axis": {"tickCount": "day"}` |
| the axis runs past your `domain` | the scale rounds to nice values | `"nice": false` |
| warning "Domains that should be unioned has conflicting sort properties" | `"sort": "-x"` on an encoding shared by layers | `"sort": {"op": "sum", "field": "<value>", "order": "descending"}` |
| `Cannot read properties of undefined` | a wrong mark or key; vega-lite does not check the schema | compare with a recipe |
| the chat refuses the chart | `data.url` or a `lookup` from a URL | inline the rows in `data.values` |
| a facet chart is narrow | multi-view charts do not fill the column | set `width` in `spec` |
