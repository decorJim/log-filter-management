import re
from collections import Counter

import logging

logger = logging.getLogger("mainLogger")

#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.INFO)

'''mask les valeurs uniques dans les messages pour les regrouper'''

# ouvre le fichier en mode de lecture
with open('HDFS_2k.log', 'r') as f:
    # Read the log file content
    content = f.read()

# remplacer les pattern par <*> information dynamique

# pas de time stamp dans le fichier de log mais si oui format 2020-12-02 13:23:03
# content \.\d+= re.sub(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', '<*>', content)

# dfs.FSNamesystem ou NameSystem.addStoredBlock
content = re.sub(r'([A-Za-z]+\.[A-Za-z]+)', '<*>', content)

# les adresses IP 10.251.43.115:50010
content = re.sub(r'(\d+\.\d+\.\d+\.\d+):(\d+)', '<*>', content)

# la partie numerique de blk_-5009020203888190378
content = re.sub(r'(\d{19})', '<*>', content)

# les adresses IP 10.251.43.115
content=re.sub(r'(\d+\.\d+\.\d+\.\d+)','<*>',content)


# Write the modified log file content to a new file
with open('logsData-masked.log', 'w') as f:
    f.write(content)

''' filtre les messages par niveau de log et compte le nombre de chaque niveau '''

# Open the file in read mode
with open('logsData-masked.log', 'r') as file:

    # read all lines in file
    lines = file.readlines()

    # Map to store log level as key and message count as value
    log_level_counts = {}

    # Define a regular expression pattern to match log levels
    log_level_pattern = r'\s(INFO|WARN|DEBUG|ERROR|CRITICAL)\s'

    # Loop through all lines in the file
    for line in lines:
        # Check if the line is not empty
        if line:
            # Cherche le pattern et trouve sa valeur comme le pattern a juste 1 composante sans - group(1)
            log_level = re.search(log_level_pattern, line).group(1)

            # Si log level deja dans la map en tant que key
            if log_level in log_level_counts:
                logger.debug("log level:"+log_level+" is in map")
                # incrementer le compteur
                log_level_counts[log_level] += 1
            else:
                logger.debug("log level:" + log_level + " is not in map")
                # sinon mettre le log level en tant que key et lui assigne la valeur de 1
                log_level_counts[log_level] = 1

    # items() retourne une copie du contenu dans la map et on affiche chaque log associe a son compte
    for log_level, count in log_level_counts.items():
        # Print the log level and its count
        logger.info(f"{log_level}: {count}")

''' trouve le pattern du message qui apparait le plus souvent '''
# Open the file in read mode
with open('logsData-masked.log', 'r') as file:
  # Read all lines in the file
  lines = file.readlines()

# Map avec le message en tant que key et le nombre de repetitions en tant que value
pattern_counts = {}

# Loop through all lines in the file
for line in lines:
  # Check if the line is not empty
  if line:

    message_pattern = line.split(' ')[4:]
    message_pattern = ' '.join(message_pattern)

    # commence a couper a partir du quatrieme element dans la ligne
    # "081109 203615 148 INFO <*>$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating"
    # ['<*>$PacketResponder:', 'PacketResponder', '1', 'for', 'block', 'blk_38865049064139660', 'terminating']
    # '<*>$PacketResponder: PacketResponder 1 for block blk_38865049064139660 terminating'

    # Check if the message pattern is already in the dictionary
    if message_pattern in pattern_counts:
      logger.debug("message "+message_pattern+" is in map")
      # Increment the count of the message pattern if it is already in the dictionary
      pattern_counts[message_pattern] += 1
    else:
      logger.debug("message "+message_pattern+" is not in map")
      # Add the message pattern to the dictionary with a count of 1 if it is not already in the dictionary
      pattern_counts[message_pattern] = 1

# Sort the dictionary by the counts in descending order
sorted_pattern_counts = {k: v for k, v in sorted(pattern_counts.items(), key=lambda item: item[1], reverse=True)}

# Get the message pattern with the highest count
most_frequent_pattern = list(sorted_pattern_counts.keys())[0]
# Print the message pattern with the highest count
logger.info("most frequest message:"+most_frequent_pattern)
logger.info('with count %d',sorted_pattern_counts[most_frequent_pattern])
logger.info("%s",True)
''' trouve toutes les messages uniques '''
# Open the file in read mode
with open('logsData-masked.log', 'r') as f:
    # Read the file's contents
    lines = f.readlines()

# Create an empty set to store the unique messages
unique_messages = set()

# Loop through the file's lines
for line in lines:
    # strip enleve les espaces au debut et a la fin d'un caractere
    unique_messages.add(line.strip())

# Print the unique messages
for message in unique_messages:
    logger.info(message)
