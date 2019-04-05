# -*- coding: utf-8 -*-
from unittest import TestCase
from data_processing import keep_material_in_place, load_config
from os import path


class TestConfiguration(TestCase):

    def setUp(self):
        self.resource_folder = path.abspath('resources');
        self.input_files = [path.join(self.resource_folder, item) for item in [u'relations.xlsx', u'signals.xlsx']];
        self.not_exist_files = [path.join(self.resource_folder, item) for item in [u'fire.xlsx', u'water.xlsx']];
        self.config_file = path.join(self.resource_folder, "config.json")

    def test_keep_material_in_place(self):
        self.assertRaises(Exception, callable=keep_material_in_place, *self.not_exist_files)
        self.assertEqual(True, keep_material_in_place(self.input_files))

    def test_load_config(self):
        expected = {
            "a": "fire",
            "b": "water",
            "c": [1, 3, 5]
        }

        data = load_config(self.config_file)

        self.assertEqual(expected['a'], data['a'])
        self.assertEqual(expected['b'], data['b'])
        self.assertEquals(expected['c'], data['c'])