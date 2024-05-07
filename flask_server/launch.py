import argparse

from agent import Agent


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Launch the Computer Science Bot.')
    parser.add_argument('--config', default="configs/t5.yaml", type=str, help='Path to the config file')
    parser.add_argument('--checkpoint', required=True, type=str, help='Path to model checkpoint.',default='/Users/dheerajvajjarapu/wikit5-v2')

    args: argparse.Namespace = parser.parse_args()

    agent = Agent(**vars(args))
    agent.run()




