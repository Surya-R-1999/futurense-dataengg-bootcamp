#!/bin/sh
touch cities.txt
echo "Enter city names, Press Ctrl+d when done:"
cat > cities.txt
cat cities.txt | sed 's/New/Old/gi' > old-cities.txt
