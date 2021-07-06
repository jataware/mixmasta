#!/usr/bin/env python

"""Tests for `mixmasta` package."""


import unittest
import warnings

from click.testing import CliRunner

import json
from mixmasta import cli, mixmasta
from pandas.util.testing import assert_frame_equal
import pandas as pd

class TestMixmaster(unittest.TestCase):
    """Tests for `mixmasta` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        #warnings.simplefilter('ignore', category=ResourceWarning)
        pass

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_001_process(self):
        """Test ISO2 primary_geo; build a date day, month, year; no primary_date; feature qualifies another feature."""

        # Define mixmasta inputs:
        mp = 'inputs/test1_input.json'
        fp = 'inputs/test1_input.csv'
        geo = 'admin3'
        outf = 'outputs/unittests'

        # Process:
        df, dct = mixmasta.process(fp, mp, geo, outf)

        # Load expected output:
        output_df = pd.read_csv('outputs/test1_output.csv', index_col=False)
        output_dict = dict
        with open('outputs/test1_dict.json') as f:
            output_dict = json.loads(f.read())

        # Sort both data frames and reindex for comparison,.
        cols = ['timestamp','country','admin1','admin2','admin3','lat','lng','feature','value']
        df.sort_values(by=cols, inplace=True)
        output_df.sort_values(by=cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        output_df.reset_index(drop =True, inplace=True)

        assert_frame_equal(df, output_df)


    # def test_command_line_interface(self):
    #     """Test the CLI."""
    #     runner = CliRunner()
    #     result = runner.invoke(cli.main)
    #     assert result.exit_code == 0
    #     assert "mixmasta.cli.main" in result.output
    #     help_result = runner.invoke(cli.main, ["--help"])
    #     assert help_result.exit_code == 0
    #     assert "--help  Show this message and exit." in help_result.output

    #     self.assertEqual(1,1)

if __name__ == '__main__':
    unittest.main()


"""
Test by: > /usr/bin/python3.8 -m unittest /workspaces/mixmasta/tests/test_mixmasta.py
"""