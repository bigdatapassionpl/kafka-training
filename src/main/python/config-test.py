import configparser
import sys

print("All arguments:", sys.argv)
if len(sys.argv) > 1:
    configPath = sys.argv[1]
    print(f"configPath: {configPath}")
else:
    print("Wrong number of arguments!")
    sys.exit(1)

config = configparser.ConfigParser()
config.read(configPath)

# Print all sections and their contents
for section_name in config.sections():
    print(f"[{section_name}]")
    for key, value in config[section_name].items():
        print(f"{key} = {value}")
    print()  # Empty line between sections
