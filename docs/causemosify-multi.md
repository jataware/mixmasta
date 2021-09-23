
## CLI: causemosify-multi

### Arguments

`causemosify-multi` takes 3 arguments:

- `--inputs`: string of JSON array of causemosify parameters in quotations with delimited interior quotations
- `--geo`: e.g. admin2, admin3. Used for all submitted files.
- `--output-file`: filename for parquets, defaults to "mixmasta_output"


### Returns

*Nothing*

### Output Files

- `{output-file}.{i}.parquet.gzip`
- `{output-file}_str.{i}.parquet.gzip`

where `{i}` is the 1-based iterator of the files passed in `--inputs`. It returns one set of parquet files for each input file.

### Inputs Example

`--inputs` interior quotation marks should be delimited:

```
"[{\"input_file\": \"build-a-date-qualifier_*.csv\", \"mapper\": \"build-a-date-qualifier.json\"}, {\"input_file\": \"raw_excel_timestampfeature.xlsx\", \"mapper\": \"mixmasta_ready_annotations_timestampfeature.json\"}]" 
```

### Testing

Build the image:
```
/mixmasta$ docker build -t mixmasta .
```

Run causemosify. Here we run the command from a directory that contains the file `Test1_2D-Q.nc` and the indicated mapper file in the `mappers` directory. Both the local directory and `mappers` directory are mounted as volumes.
```
$ docker run -v ${PWD}:/tmp -v ${PWD}/mappers:/mappers mixmasta:latest causemosify-multi --inputs="[{\"input_file\":\"/tmp/Test1_2D-Q.nc\", \
	  \"mapper\":\"./mappers/mapper_e489d023-58c3-4544-8ae1-bb9cfa987e23.json\"}]
```
