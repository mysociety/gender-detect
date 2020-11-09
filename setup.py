from distutils.core import setup
setup(
  name = 'gender-detect',
  packages = ['gender_detect',
              ],
  package_dir={'gender_detect': 'gender_detect'},
  package_data  = {"gender_detect": ['assets/processed/*.csv']},
  version = '0.1',
  description = 'Tool for adding gender based on name data with pandas',
  author = 'Alex Parsons',
  author_email = 'alex@alexparsons.co.uk',
  url = 'https://github.com/mysociety/gender-detect', 
  download_url = '', 
  keywords = [], 
  classifiers = [],
)