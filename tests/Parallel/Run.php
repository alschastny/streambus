<?php

require __DIR__ . '/../../vendor/autoload.php';

$argv = $_SERVER['argv'];
array_shift($argv);
$handlerClass = array_shift($argv);
(new $handlerClass())(...$argv);
