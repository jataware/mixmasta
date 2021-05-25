docker run --rm -v $PWD:/tmp \
           mixmasta \
           causemosify \
           --input_file=/tmp/test_file_2.csv \
           --mapper=/tmp/test_file_2.json \
           --geo=admin2 \
           --output_file=/tmp/test_file_2