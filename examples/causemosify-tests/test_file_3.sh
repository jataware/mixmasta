docker run --rm -v $PWD:/tmp \
           mixmasta-local \
           causemosify \
           --input_file=/tmp/test_file_3.csv \
           --mapper=/tmp/test_file_3.json \
           --geo=admin2 \
           --output_file=/tmp/test_file_3