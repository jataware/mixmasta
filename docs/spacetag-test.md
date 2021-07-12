
# Package testing in SpaceTag

After running unittests and building the mixmasta package, it might be a good
idea to test in the spacetag environment, in particular to confirm any datafiles,
e.g. for iso2 lookup, are loading properly.


## Running SpaceTag
Clone SpaceTag from its github  [repo](https://github.com/jataware/spacetag). You may need
to modify the .env file to get it to build. Then create the docker container as described in
the README:
```
docker-compose up --build -d
```

Get the container_id (in this example it is `7b71fc24e8d4`) and run bash in the container:
```
docker ps
docker exec -it 7b71fc24e8d4 bash
```

## Build MixMasta and install in the SpaceTag container
Remember to clean out `/dist` before building. In this example we are at version 0.5.15:
```
source\repos\mixmasta>python3 -m build
```
Move the `.whl` file:
```
mixmasta\dist>docker cp mixmasta-0.5.15-py2.py3-none-any.whl 7b71fc24e8d4:/root/mixmasta-0.5.15-py2.py3-none-any.whl
```
Uninstall any old installs:
```
root@7b71fc24e8d4:~# pip uninstall mixmasta
```
Install the `.whl` file:
```
root@7b71fc24e8d4:~# pip install mixmasta-0.5.15-py2.py3-none-any.whl
```
The output of the above command should confirm the `.whl` is being processed:
```
Processing ./mixmasta-0.5.15-py2.py3-none-any.whl
```

## Copy test data and schema files into the container and test
Here we have test files in mixmasta\examples\causemosify-tests that we have setup so that
the `primary_geo` is an `ISO3` code that should be converted to GADM country name via a
local data file.
```
mixmasta\examples\causemosify-tests>docker cp raw_excel_timestampfeature.xlsx 7b71fc24e8d4:/root/raw_excel_timestampfeature.xlsx
mixmasta\examples\causemosify-tests>docker cp mixmasta_ready_annotations_timestampfeature.json 7b71fc24e8d4:/root/mixmasta_ready_annotations_timestampfeature.json
```
Test the MixMasta package in the Python interpretor:
```
root@7b71fc24e8d4:~# python3
```
Paste the following into the interpretor:
```
from mixmasta import mixmasta as mix
mp = 'mixmasta_ready_annotations_timestampfeature.json'
fp = 'raw_excel_timestampfeature.xlsx'
geo = 'admin3'
outf = 'testing'
df, dct = mix.process(fp, mp, geo, outf)
dct
df.head(50)
```
Success confirms MixMasta is installed. Examine the dataframe output to confirm it is correct.

Mixmata can also be run from the CLI:
```
root@20abb83059d3:~# mixmasta causemosify --input_file=/root/raw_excel.xlsx --mapper=/root/mapper.json --geo=admin3
```

The following commands in the Python interpretor will examine the output parquet file:
```
import pandas as pd
df2 = pd.read_parquet("mixmasta_output.parquet.gzip")
df2.tail(50)

```
