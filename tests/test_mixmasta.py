#!/usr/bin/env python

"""Tests for `mixmasta` package."""


import unittest
import warnings

from click.testing import CliRunner

import json
from mixmasta import cli, mixmasta
from pandas.util.testing import assert_frame_equal, assert_dict_equal
import pandas as pd
import gc
import psutil

class TestMixmaster(unittest.TestCase):
    """Tests for `mixmasta` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        #with warnings.catch_warnings():
        warnings.simplefilter('ignore')

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_001_process(self):
        """Test ISO2 primary_geo; build a date day, month, year; no primary_date; feature qualifies another feature."""

        # Define mixmasta inputs:
        mp = 'inputs/test1_input.json'
        fp = 'inputs/test1_input.csv'
        geo = 'admin2'
        outf = 'outputs/unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv('outputs/test1_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open('outputs/test1_dict.json') as f:
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
        mp = 'inputs/test2_assetwealth_input.json'
        fp = 'inputs/test2_assetwealth_input.tif'
        geo = 'admin2'
        outf = 'outputs/unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)
        #categories = df.select_dtypes(include=['category']).columns.tolist()
        df['value'] = df['value'].astype('str')

        # Load expected output:
        output_df = pd.read_csv('outputs/test2_assetwealth_output.csv', index_col=False)

        with open('outputs/test2_assetwealth_dict.json') as f:
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
        output_df.reset_index(drop =True, inplace=True)
        output_df.sort_values(by=cols, inplace=True)

        # Assertions
        assert_frame_equal(df, output_df, check_categorical = False)
        assert_dict_equal(dct, output_dict)

    def test_003_process(self):
        """Test qualifies, lat/lng primary geo."""

        # Define mixmasta inputs:
        mp = 'inputs/test3_qualifies.json'
        fp = 'inputs/test3_qualifies.csv'
        geo = 'admin2'
        outf = 'outputs/unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv('outputs/test3_qualifies_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open('outputs/test3_qualifies_dict.json') as f:
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
        mp = 'inputs/test4_rainfall_error.json'
        fp = 'inputs/test4_rainfall_error.xlsx'
        geo = 'admin2'
        outf = 'outputs/unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv('outputs/test4_rainfall_error_output.csv', index_col=False)
        output_df = mixmasta.optimize_df_types(output_df)
        with open('outputs/test4_rainfall_error_dict.json') as f:
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

    """
    def test_command_line_interface(self):
         #Test the CLI.
         runner = CliRunner()
         result = runner.invoke(cli.main)
         assert result.exit_code == 0
         assert "mixmasta.cli.main" in result.output
         help_result = runner.invoke(cli.main, ["--help"])
         assert help_result.exit_code == 0
         assert "--help  Show this message and exit." in help_result.output

         self.assertEqual(1,1)
    """

if __name__ == '__main__':
    unittest.main()



"""
Test by: > /usr/bin/python3.8 -m unittest /workspaces/mixmasta/tests/test_mixmasta.py -v
"""