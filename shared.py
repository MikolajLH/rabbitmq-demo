from enum import Enum, auto

EXCHANGE_NAME = "exchange1"

class Base(Enum):
    @classmethod
    def from_string(cls, value: str):
        for member in cls:
            if member.value == value.upper():
                return member
        raise ValueError(f"{value} is not a valid {cls.__name__}")
    
    def _generate_next_value_(name, start, count, last_values):
        return name

    def __str__(self):    
        return f"{self.value}".lower()


class Gear(Base):
    TLEN = auto()
    BUTY = auto()
    PLECAK = auto()


class Mode(Base):
    TEAM = auto()
    SUPPLIER = auto()
    ALL = auto()


def get_gear_q_name(gear):
    return f"q-gear.{gear}"

def get_gear_routing_key(gear, team="*"):
    return f"{team}.gear.{gear}"


def get_result_q_name(team_name):
    return f"q-result.{team_name}"

def get_result_routing_key(team_name = "*", supplier_name = "*"):
    return f"{supplier_name}.result.{team_name}"


def get_admin_q_name(name, mode):
    return f"q-admin.{mode}-{name}"

def get_admin_routing_key(mode):
    return f"admin.{mode}"


def admin_callback(ch, method, properties, body):
    msg = body.decode("utf-8")
    _, mode = method.routing_key.split(".")
    print()
    print(f"New message matched to routing key {method.routing_key}")
    print(f"Admin to <{mode}> sent: {msg}")
    print()
    
    ch.basic_ack(delivery_tag=method.delivery_tag)