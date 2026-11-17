
import argparse
from pathlib import Path
from datasets import load_dataset


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    output = Path(args.output)
    if output.exists():
        parser.error("Output exists; choose a new path")
    output.parent.mkdir(parents=True, exist_ok=True)

    dataset = load_dataset("fancyzhx/ag_news", split="train")
    dataset.to_parquet(str(output))

    print(f"Downloaded {len(dataset)} articles")
    print(f"Saved dataset to: {output}")


if __name__ == "__main__":
    main()
