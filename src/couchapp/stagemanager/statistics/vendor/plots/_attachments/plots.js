function maxY(data){

  var totElements = data.length;

  var maxval = 0;
  for(var i = 0; i < totElements; ++i){
    if (data[i].y > maxval)  maxval = data[i].y;
  }
  return maxval;
}

function pie(data, canvas, labels) {

 var w = 400,
     h = 400,
     r = w / 2;

sum = pv.sum(data);

var vis = new pv.Panel()
    .canvas(canvas || 'pie')
    .width(w + 10)
    .height(w + 20);

 vis.add(pv.Label)
    .left(r)
    .top(15)
    .textAlign("center")
    .text("Staged, incomplete, and failed data status");


var wedge = vis.add(pv.Wedge)
    .data(data)
    .left(w/2)
    .bottom(w/2)
    .innerRadius(r - 80)
    .outerRadius(r)
    .angle(function(d) { return d / sum * 2 * Math.PI; })
    .event("mouseover", function() { return this.innerRadius(0); })
    .event("mouseout", function() { return this.innerRadius(r - 80); });

wedge.add(pv.Label)
  .visible(function(d) { return d > .15; })
  .left(function() { return r/1.1 * Math.cos(wedge.midAngle()) + r; })
  .bottom(function() { return -r/1.1 * Math.sin(wedge.midAngle()) + r; })
  .textAlign("center")
  .textBaseline("middle");

wedge.add(pv.Label)
  .visible(function(d) { return d > .15; })
  .textAngle(0)
  .left(function() { return r/1.2 * Math.cos(wedge.midAngle()) + r; })
  .bottom(function() { return -r/1.2 * Math.sin(wedge.midAngle()) + r; })
  .textAlign("center")
  .text(function(d) {
    if (labels) {
      return labels[this.index];
    } else {
      return d.toFixed(2);
    }
  });

vis.render();

}

function bar(data, canvas) {
  /* Sizing and scales. */

  var w = 400,
      h = 250,
      x = pv.Scale.linear(0, 200).range(0, w),
      y = pv.Scale.ordinal(pv.range(10)).splitBanded(0, h, 4/5);

  /* The root panel. */
  var vis = new pv.Panel()
      .canvas(canvas || 'bar')
      .width(w)
      .height(h)
      .bottom(20)
      .left(20)
      .right(10)
      .top(5);

  /* The bars. */
  var bar = vis.add(pv.Bar)
      .data(data)
      .top(function() { return y(this.index); })
      .height(y.range().band)
      .left(0)
      .width(x);

  /* The value label. */
  bar.anchor("right").add(pv.Label)
      .textStyle("white")
      .text(function(d) { return d.toFixed(1); });

  /* The variable label. */
  bar.anchor("left").add(pv.Label)
      .textMargin(5)
      .textAlign("right")
      .text(function() { return "ABCDEFGHIJK".charAt(this.index); });

  /* X-axis ticks. */
  vis.add(pv.Rule)
      .data(x.ticks(5))
      .left(x)
      .strokeStyle(function(d) { return d ? "rgba(255,255,255,.3)" : "#000"; })
    .add(pv.Rule)
      .bottom(0)
      .height(5)
      .strokeStyle("#000")
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);

  vis.render();
}

function area(data, canvas, title){

  /* Sizing and scales. */

  var y_max = maxY(data) * 1.2;

  var w = 500,
      h = 250,
      x = pv.Scale.linear(data, function(d) { return d.x; }).range(0, w),
      y = pv.Scale.linear(0, y_max).range(0, h);

  /* The root panel. */
  var vis = new pv.Panel()
      .canvas(canvas || 'area')
      .width(w)
      .height(h)
      .bottom(60)
      .left(60)
      .right(10)
      .top(15);

  /* Add title*/
  vis.add(pv.Label)
    .left(w/2)
    .top(0)
    .textAlign("center")
    .text(title);

  /* Put label on x axis*/
  vis.add(pv.Label)
    .left(w/2)
    .bottom(-30)
    .textAlign("center")
    .text("Stage iteration");

    /* Put label on y axis*/
  vis.add(pv.Label)
    .left(-30)
    .top(h/2)
    .textAlign("center")
    .text("Time (secs)")
    .textAngle(-Math.PI / 2);


  /* Y-axis and ticks. */
  vis.add(pv.Rule)
      .data(y.ticks(5))
      .bottom(y)
      .strokeStyle(function(d) { return d ? "#eee" : "#000"; })
    .anchor("left").add(pv.Label)
      .text(y.tickFormat);

  /* X-axis and ticks. */
  vis.add(pv.Rule)
      .data(x.ticks())
      .visible(function(d) { return d; })
      .left(x)
      .bottom(-5)
      .height(5)
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);

  /* The area with top line. */
  vis.add(pv.Area)
      .data(data)
      .bottom(1)
      .left(function(d) { return x(d.x); })
      .height(function(d) { return y(d.y); })
      .fillStyle("rgb(121,173,210)")
    .anchor("top").add(pv.Line)
      .lineWidth(3);

  vis.render();


}

function stacked(data, canvas){


  /* Sizing and scales. */
   var y_max = (Math.round(data[0][0].y) + data[1][0].y + data[2][0].y) * 1.3;

  var w = 600,
      h = 300,
      //x = pv.Scale.linear(data, function(d) { return d[0].x; }).range(0, w),
      x = pv.Scale.linear(1, 10).range(0, w),
      y = pv.Scale.linear(0, y_max).range(0, h);

  /* The root panel. */
  var vis = new pv.Panel()
      .canvas(canvas || 'stacked')
      .width(w)
      .height(h)
      .bottom(40)
      .left(60)
      .right(10)
      .top(20);

  /* Title label */
  vis.add(pv.Label)
      .left(w/2)
      .top(15)
      .textAlign("center")
      .text("Staged, incomplete, and failed files per iteration");

  /* Put label on x axis*/
  vis.add(pv.Label)
    .left(w/2)
    .bottom(-30)
    .textAlign("center")
    .text("Stage iteration");

    /* Put label on y axis*/
  vis.add(pv.Label)
    .left(-30)
    .bottom(h/2)
    .textAlign("center")
    .text("Number of files")
    .textAngle(-Math.PI / 2);

  /* X-axis and ticks. */
  vis.add(pv.Rule)
      .data(x.ticks())
      .visible(function(d) { return d; })
      .left(x)
      .bottom(-5)
      .height(5)
    .anchor("bottom").add(pv.Label)
      .text(x.tickFormat);

  /* The stack layout. */
  vis.add(pv.Layout.Stack)
      .layers(data)
      .x(function(d) { return x(d.x); })
      .y(function(d) { return y(d.y); })
    .layer.add(pv.Area);

  /* Y-axis and ticks. */
  vis.add(pv.Rule)
      .data(y.ticks(3))
      .bottom(y)
      .strokeStyle(function(d) { return d ? "rgba(128,128,128,.2)" : "#000"; })
    .anchor("left").add(pv.Label)
      .text(y.tickFormat);

  vis.render();
}
