import os
import re

for root, _, files in os.walk("../../../output/data5/", topdown=False):
    for name in files:
        if name.startswith("data"):
            print(os.path.join(root, name))
            try:
                with open("../../../output/datafix/"+name, "wb") as out:
                    with open(os.path.join(root, name), encoding="utf-8") as fp:
                        for line in fp:
                            newline = re.sub("</HTML>", "</HTMLSOURCE>", line)
                            out.write(newline.encode("UTF-8"))
            except UnicodeDecodeError:
                print("Unicode Error: ", name)
