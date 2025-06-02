import configparser

config = configparser.ConfigParser()
config.read('/Users/radek/programs/kafka/config.properties')

# Print all sections and their contents
for section_name in config.sections():
    print(f"[{section_name}]")
    for key, value in config[section_name].items():
        print(f"{key} = {value}")
    print()  # Empty line between sections
