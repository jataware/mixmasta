docker run --rm -v $PWD:/tmp \
           mixmasta-local \
           causemosify \
           --input_file=/tmp/test_file_1.csv \
           --mapper=/tmp/test_file_1.json \
           --geo=admin2 \
           --output_file=/tmp/test_file_1