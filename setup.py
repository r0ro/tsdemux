import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tsdemux",
    version="0.0.1",
    author="Romain Fliedel",
    author_email="romain.fliedel@gmail.com",
    description="A simple MPEG2 TS demuxer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/r0ro/tsdemux",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Topic :: Multimedia",
    ],
    install_requires=[
        'colorlog'
    ],
    python_requires='>=3.6',
)
