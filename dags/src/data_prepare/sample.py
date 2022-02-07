from src.utils.configs import EnvData


def display():
    env_data = EnvData()
    print('can call .env file here')
    print(f"host name is: {env_data.get_values()['host']}")
    print(f"Database name is: {env_data.get_values()['dbname']}")
    print(f"Database user is: {env_data.get_values()['user']}")

if __name__=="__main__":
    display()