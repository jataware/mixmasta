#!/usr/bin/env python

"""Tests for `mixmasta` package."""


import unittest
import warnings

from click.testing import CliRunner
import subprocess
import logging
import json
from mixmasta import cli, mixmasta
from pandas.util.testing import assert_frame_equal, assert_dict_equal
import pandas as pd
import gc
import os

if os.name == 'nt':
    sep = '\\'
else:
    sep = '/'

logger = logging.getLogger(__name__)

class TestMixmaster(unittest.TestCase):
    """Tests for `mixmasta` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

        # turn off warnings unless refactoring.
        warnings.simplefilter('ignore')

    def tearDown(self):
        """Tear down test fixtures, if any."""
        gc.collect()
        # Delete any output parquet files.
        try:
            os.remove(f"outputs{sep}unittests.parquet.gzip")
        except:
            pass

        try:
           os.remove(f"outputs{sep}unittests_str.parquet.gzip")
        except:
            pass

    def test_001_process(self):
        """Test ISO2 primary_geo; build a date day, month, year; no primary_date; feature qualifies another feature."""

        # Define mixmasta inputs:
        mp = f"inputs{sep}test1_input.json"
        fp = f"inputs{sep}test1_input.csv"
        geo = 'admin2'
        outf = f"outputs{sep}unittests"

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test1_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open(f'outputs{sep}test1_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Assertions
        assert_frame_equal(df, output_df)
        assert_dict_equal(dct, output_dict)

    def test_002_process(self):
        """
        Test GeoTiff This tests that multi-band geotiff processing is the same. Uses the
        asset_wealth tif which has 4 bands of different years representing a
        measure of wealth.
        """

        # Define mixmasta inputs:
        mp = f'inputs{sep}test2_assetwealth_input.json'
        fp = f'inputs{sep}test2_assetwealth_input.tif'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)
        #categories = df.select_dtypes(include=['category']).columns.tolist()
        df['value'] = df['value'].astype('str')

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test2_assetwealth_output.csv', index_col=False)

        with open(f'outputs{sep}test2_assetwealth_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df = df[cols]
        output_df = output_df[cols]

        # Optimize datatypes for output_df.
        floats = output_df.select_dtypes(include=['float64']).columns.tolist()
        output_df[floats] = output_df[floats].apply(pd.to_numeric, downcast='float')

        ints = output_df.select_dtypes(include=['int64']).columns.tolist()
        output_df[ints] = output_df[ints].apply(pd.to_numeric, downcast='integer')

        # Standardize value and feature columns to str for comparison.
        df['value'] = df['value'].astype('str')
        df['feature'] = df['feature'].astype('str')
        output_df['value'] = output_df['value'].astype('str')
        output_df['feature'] = output_df['feature'].astype('str')

        # Sort and reindex.
        df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        output_df.sort_values(by=cols, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_003_process(self):
        """Test qualifies, lat/lng primary geo."""

        # Define mixmasta inputs:
        mp = f'inputs{sep}test3_qualifies.json'
        fp = f'inputs{sep}test3_qualifies.csv'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test3_qualifies_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open(f'outputs{sep}test3_qualifies_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)

        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Make the datatypes the same for value/feature columns.
        df['value'] = df['value'].astype('str')
        df['feature'] = df['feature'].astype('str')
        output_df['value'] = output_df['value'].astype('str')
        output_df['feature'] = output_df['feature'].astype('str')

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_004_process(self):
        """Test .xlxs file, qualifies col with multi dtypes."""

        # Define mixmasta inputs:
        mp = f'inputs{sep}test4_rainfall_error.json'
        fp = f'inputs{sep}test4_rainfall_error.xlsx'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test4_rainfall_error_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open(f'outputs{sep}test4_rainfall_error_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value','MainCause']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)

        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Make the datatypes the same for value/feature and qualifying columns.
        df['value'] = df['value'].astype('str')
        df['feature'] = df['feature'].astype('str')
        df['MainCause'] = df['MainCause'].astype('str')
        output_df['value'] = output_df['value'].astype('str')
        output_df['feature'] = output_df['feature'].astype('str')
        output_df['MainCause'] = output_df['MainCause'].astype('str')

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_005__command_line_interface(self):
        """Test the CLI and causemosify-multi."""
        """
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert "mixmasta.cli.main" in result.output
        help_result = runner.invoke(cli.main, ["--help"])
        assert help_result.exit_code == 0
        assert "--help  Show this message and exit." in help_result.output

        """

        # Confirm CLI --help is available.
        result = subprocess.run(['mixmasta', '--help'], capture_output=True, encoding='utf-8')
        self.assertIn('Processor for generating CauseMos compliant datasets.', result.stdout)
        self.assertIn('--help  Show this message and exit.', result.stdout)
        self.assertEqual(result.returncode, 0)
        self.assertIn('causemosify-multi  Process multiple input files to generate a single',result.stdout)


        # Confirm CLI causemosify-multi --help is available.
        result = subprocess.run(['mixmasta', "causemosify-multi", "--help"], capture_output=True, encoding='utf-8')
        self.assertEqual(result.returncode, 0)
        self.assertIn('Process multiple input files to generate a single CauseMos compliant',result.stdout)

        # Run causemosify-multi
        inputs = "--inputs=[{\"input_file\": \"inputs" + f"{sep}test1_input.csv\",\"mapper\": \"inputs{sep}test1_input.json\"" + "},{\"input_file\": \""
        inputs = inputs + f"inputs{sep}test3_qualifies.csv\",\"mapper\": \"inputs{sep}test3_qualifies.json\"" + "}]"
        result = subprocess.run(['mixmasta', "causemosify-multi", inputs, "--geo=admin2", f"--output-file=outputs{sep}unittests"], capture_output=True, encoding='utf-8')
        if (result.returncode != 0):
            print(result)
        self.assertEqual(result.returncode, 0)
        
        ## Compare parquet files.
        df1 = pd.read_parquet(f"outputs{sep}unittests.1.parquet.gzip")
        df2 = pd.read_parquet(f"outputs{sep}unittests.2.parquet.gzip")
        df = df1.append(df2)
        output_df = pd.read_parquet(f"outputs{sep}test5.parquet.gzip")

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        logger.info(df.shape)
        logger.info(output_df.shape)

        # Assertion
        assert_frame_equal(df, output_df, check_categorical = False)

        ## Compare str.parquet file.
        df = pd.read_parquet(f"outputs{sep}unittests_str.2.parquet.gzip")
        output_df = pd.read_parquet(f"outputs{sep}test5_str.parquet.gzip")

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Assertion
        assert_frame_equal(df, output_df, check_categorical = False)

    def test_006_process(self):
        """Test multi primary_geo, resolve_to_gadm"""

        # Define mixmasta inputs:
        mp = f'inputs{sep}test6_hoa_conflict_input.json'
        fp = f'inputs{sep}test6_hoa_conflict_input.csv'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test6_hoa_conflict_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open(f'outputs{sep}test6_hoa_conflict_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)

        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        # Make the datatypes the same for value/feature and qualifying columns.
        df['value'] = df['value'].astype('str')
        df['feature'] = df['feature'].astype('str')
        output_df['value'] = output_df['value'].astype('str')
        output_df['feature'] = output_df['feature'].astype('str')

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_007_single_band_tif(self):
        """ This tests single-band geotiff processing."""

        # Define mixmasta inputs:
        mp = f'inputs{sep}test7_single_band_tif_input.json'
        fp = f'inputs{sep}test7_single_band_tif_input.tif'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)
        #categories = df.select_dtypes(include=['category']).columns.tolist()
        df['value'] = df['value'].astype('str')

        # Load expected output:
        output_df = pd.read_csv(f'outputs{sep}test7_single_band_tif_output.csv', index_col=False)

        with open(f'outputs{sep}test7_single_band_tif_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df = df[cols]
        output_df = output_df[cols]

        # Optimize datatypes for output_df.
        floats = output_df.select_dtypes(include=['float64']).columns.tolist()
        output_df[floats] = output_df[floats].apply(pd.to_numeric, downcast='float')

        ints = output_df.select_dtypes(include=['int64']).columns.tolist()
        output_df[ints] = output_df[ints].apply(pd.to_numeric, downcast='integer')

        # Standardize value and feature columns to str for comparison.
        df['value'] = df['value'].astype('str')
        df['feature'] = df['feature'].astype('str')
        output_df['value'] = output_df['value'].astype('str')
        output_df['feature'] = output_df['feature'].astype('str')

        # Sort and reindex.
        df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)

        output_df.sort_values(by=cols, inplace=True)
        output_df.reset_index(drop=True, inplace=True)

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_008_aliases(self):
        """ This tests feature name aliases."""

        # Define mixmasta inputs:
        mp = f'inputs{sep}test8_aliases_input.json'
        fp = f'inputs{sep}test8_aliases_input.csv'
        geo = 'admin2'
        outf = f'outputs{sep}unittests'

        inputs = "--inputs=[{\"input_file\": \"inputs" + f"{sep}test8_aliases_input.csv\",\"mapper\": \"inputs{sep}test8_aliases_input.json\"" + "}]"
        result = subprocess.run(['mixmasta', "causemosify-multi", inputs, "--geo=admin2", f"--output-file=outputs{sep}unittests"], capture_output=True, encoding='utf-8')

        if (result.returncode != 0):
            print(result)
        self.assertEqual(result.returncode, 0)

        ## Compare parquet files.
        df1 = pd.read_parquet(f"outputs{sep}unittests.1.parquet.gzip")
        df2 = pd.read_parquet(f"outputs{sep}unittests_str.1.parquet.gzip")
        df = df1.append(df2)

        output_df_1 = pd.read_parquet(f"outputs{sep}test8_aliases.parquet.gzip")
        output_df_2 = pd.read_parquet(f"outputs{sep}test8_aliases_str.parquet.gzip")
        output_df = output_df_1.append(output_df_2)

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)


if __name__ == '__main__':
    unittest.main()

"""
Test by: > /usr/bin/python3.8 -m unittest /workspaces/mixmasta/tests/test_mixmasta.py -v
"""