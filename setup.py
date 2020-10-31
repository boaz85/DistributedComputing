from setuptools import setup

setup(name='distributed_computing',
      version='0.1.0',
      description='Distributed GPU computing with python',
      packages=['distributed_computing'],
      author_email='boaz85@gmail.com',
      install_requires=['redis-server==5.0.7', 'GPUtil==1.4.0', 'psutil==5.7.3', 'setproctitle==1.1.10',
                        'twisted==20.3.0', 'redis==3.5.3', 'requests==2.23.0', 'service_identity==18.1.0'],
      scripts=['distributed_computing/start_node'],
      zip_safe=False)
