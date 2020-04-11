
import setuptools

setuptools.setup(
    name="async-slack",
    version="0.0.1",
    author="Pascal Bugnion",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": ["async-slack-update=async_slack.update_database:main"]
    }
)
