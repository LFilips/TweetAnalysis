var fill = d3.scale.category20();

d3.csv("WordFrequency.csv", function (data) {

    d3.layout.cloud().size([2000, 2000])
        .words(data)
        .padding(5)
        .rotate(function () {
            return ~~(Math.random() * 2) * 5;
        })
        .font("Impact")
        .fontSize(function (d) {
            return Math.max(8, Math.min(d.size, 48));
        })
        .on("end", draw)
        .start();

    function draw(words) {
        d3.select("body").append("svg")
            .attr("width", 2000)
            .attr("height", 2000)
            .append("g")
            .attr("transform", "translate(300,300)")
            .selectAll("text")
            .data(data)
            .enter().append("text")
            .style("font-size", function (d) {
                return d.size + "px";
            })
            .style("font-family", "Impact")
            .style("fill", function (d, i) {
                return fill(i);
            })
            .attr("text-anchor", "middle")
            .attr("transform", function (d) {
                return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
            })
            .text(function (d) {
                return d.text;
            });
    }
});