class VirtualMachine:
    def __init__(self, vm_id, ram, cpu):
        self.vm_id = vm_id
        self.ram = ram
        self.cpu = cpu

    def update(self, ram, cpu):
        self.ram = ram
        self.cpu = cpu

    def __str__(self):
        return f"VM ID: {self.vm_id}, RAM: {self.ram}, CPU: {self.cpu}"