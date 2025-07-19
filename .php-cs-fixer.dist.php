<?php
return (new PhpCsFixer\Config())
    ->setFinder(PhpCsFixer\Finder::create()
        ->in(__DIR__ . '/src')
        ->in(__DIR__ . '/tests'))
    ->setRules([
        '@PER-CS2.0' => true,
    ]);
