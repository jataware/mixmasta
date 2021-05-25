docker run --rm -v $PWD:/tmp \
           mixmasta \
           causemosify \
           --input_file=/tmp/test_file_4.csv \
           --mapper=/tmp/test_file_4.json \
           --geo=admin2 \
           --output_file=/tmp/test_file_4