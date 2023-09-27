import os
import argparse

def look(path, base, prefix):
    obj = os.scandir(path)
    res = []
    for entry in obj :
        new_path = f"{path}/{entry.name}"
        if entry.is_dir():
            res = res + look(new_path, base, prefix)
        if entry.is_file():
            res = res + [{"name": new_path.replace(base, prefix), "size": entry.stat().st_size}]

    obj.close()
    return res

def save(path, data):
    with open(path, "w") as f:
        f.write("File Path,Size\n")

        for file in data:
            f.write(f'{file["name"]},{file["size"]}\n')

def main():
    parser = argparse.ArgumentParser(
        description="Export dataset information to a CSV file."
    )
    parser.add_argument(
        "--input",
        metavar="input",
        default=".",
        type=str,
        help="The absolute path of the directory to look through",
    )
    parser.add_argument(
        "--output",
        metavar="output",
        default="res.csv",
        type=str,
        help="The absolute path of the CSV file to save the results to",
    )

    parser.add_argument(
        "--prefix",
        metavar="prefix",
        default="/app/files",
        type=str,
        help="The absolute path of the directory to look through",
    )

    args = parser.parse_args()

    save(args.output, look(args.input, args.input, args.prefix))

if __name__ == "__main__":
    main()
