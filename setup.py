# Импорт недавно установленного пакета setuptools.
import setuptools
import os


# Открытие README.md и присвоение его long_description.
with open("README.md", "r") as fh:
	long_description = fh.read()

requirements = []
with open('requirements.txt', "r") as fh:
	requirements.append(fh.readline())

# Определение requests как requirements для того, чтобы этот пакет работал. Зависимости проекта.
# requirements = ["requests<=2.21.0"]

# Функция, которая принимает несколько аргументов. Она присваивает эти значения пакету.
setuptools.setup(
	# Имя дистрибутива пакета. Оно должно быть уникальным, поэтому добавление вашего имени пользователя в конце является обычным делом.
	name="rmq_custom_pack",
	# Номер версии вашего пакета. Обычно используется семантическое управление версиями.
	version="0.0.1",
	# Имя автора.
	author="MontaGGGne",
	# Его почта.
	author_email="fredom871@gmail.com",
	# Краткое описание, которое будет показано на странице PyPi.
	description="Custom package for more convenient use of RabbitMQ functions (producer and consumer).",
	# Длинное описание, которое будет отображаться на странице PyPi. Использует README.md репозитория для заполнения.
	long_description=long_description,
	# Определяет тип контента, используемый в long_description.
	long_description_content_type="text/markdown",
	# URL-адрес, представляющий домашнюю страницу проекта. Большинство проектов ссылаются на репозиторий.
	url="https://github.com/MontaGGGne/rmq_custom_pack",
	# Находит все пакеты внутри проекта и объединяет их в дистрибутив.
	packages=setuptools.find_packages('src', include=['rpc*']),
	# requirements или dependencies, которые будут установлены вместе с пакетом, когда пользователь установит его через pip.
	install_requires=requirements,
	# Предоставляет pip некоторые метаданные о пакете. Также отображается на странице PyPi.
	classifiers=[
		"Programming Language :: Python :: 3.10",
		"License :: OSI Approved :: MIT License",
		"Operating System :: OS Independent",
	],
	# Требуемая версия Python.
	python_requires='>=3.10',
)