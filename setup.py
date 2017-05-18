from distutils.core import setup, Extension

module1 = Extension('pyrquet',
                    sources=['pyrquetmodule.cpp'],
                    include_dirs=['/usr/local/lib'],
                    libraries=['parquet'],
                    extra_compile_args=['-std=c++11'])

setup (name = 'pyrquet',
       version = '1.0',
       description = 'This is a spam package',
       ext_modules =[module1])
