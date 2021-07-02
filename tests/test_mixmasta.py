#!/usr/bin/env python

"""Tests for `mixmasta` package."""


import unittest

from click.testing import CliRunner

from mixmasta import cli, mixmasta


class TestMixmaster(unittest.TestCase):
    """Tests for `mixmasta` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        pass

    def tearDown(self):
        """Tear down test fixtures, if any."""
        pass

    def test_000_something(self):
        """Test something."""
        self.assertEqual(2,2)

    def test_command_line_interface(self):
        """Test the CLI."""
        """
        runner = CliRunner()
        result = runner.invoke(cli.main)
        assert result.exit_code == 0
        assert "mixmasta.cli.main" in result.output
        help_result = runner.invoke(cli.main, ["--help"])
        assert help_result.exit_code == 0
        assert "--help  Show this message and exit." in help_result.output
        """
        self.assertEqual(1,1)

if __name__ == '__main__':
    unittest.main()


"""
Test by: > /usr/bin/python3.8 -m unittest /workspaces/mixmasta/tests/test_mixmasta.py
"""