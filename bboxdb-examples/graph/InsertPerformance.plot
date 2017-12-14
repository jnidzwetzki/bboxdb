set autoscale
#set xrange[0:19]
#set yrange [4600:6500]
set grid x y
set key left

set xlabel "Time (sec)"
set ylabel "Number of tuples per sec"
set title "Inserting tuples into a relation"

#set size square
set format x "%10.f"

#set xtics (0, 50000. 100000, 200000, 500000, 1000000, 2000000)

plot "InsertPerformance.dat" index 0:0 using ($1/1000):3 with linespoints ti "Time" 

set terminal postscript enhanced 
set out 'insert.eps'
replot

set terminal postscript enhanced 
set out '| epstopdf --filter --autorotate=All > insert.pdf; pdfcrop --margins 10 insert.pdf > /dev/null'
replot



