import argparse
from aiopath import AsyncPath
from aioshutil import copyfile
import logging
import asyncio

parser = argparse.ArgumentParser(description="Sorting folder.")
parser.add_argument("--source", "-s", help="source folder", required=True)
parser.add_argument("--output", "-o", help="output folder", default="dist")

args = parser.parse_args()
source = AsyncPath(args.source)
output = AsyncPath(args.output)


async def grab_folder(path: AsyncPath):
    tasks = []
    async for el in path.iterdir():
        if await el.is_dir():
            tasks.append(grab_folder(el))
        else:
            tasks.append(copy_file(el))
    await asyncio.gather(*tasks)


async def copy_file(file: AsyncPath):
    ext_folder = output / file.suffix
    try:
        await ext_folder.mkdir(exist_ok=True, parents=True)
        await copyfile(file, ext_folder / file.name)
        logging.info(f"Copied {file.name} to {ext_folder}")
    except OSError as err:
        logging.error(err)

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
    logging.info("Starting file sorting...")
    try:
        await grab_folder(source)
        logging.info("File sorting completed successfully.")
        print(f"You can delete {source}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
