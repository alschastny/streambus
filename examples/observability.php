<?php

require_once __DIR__ . '/basic.php';

$info = $builder->createBusInfo();

foreach ($info->getSubjects() as $subject) {
    printf('Subject: %s' . PHP_EOL, $subject);
    printf('  Stream length: %d' . PHP_EOL, $info->getStreamLength($subject));
    foreach ($info->getGroups($subject) as $group) {
        printf('  Group: %s' . PHP_EOL, $group);
        printf('    Pending: %d' . PHP_EOL, $info->getGroupPending($group, $subject));
        printf('    Time lag: %d' . PHP_EOL, $info->getGroupPending($group, $subject));
    }
}

foreach ($info->getSubjects() as $subject) {
    $xinfo = $info->xinfo($subject);
    printf('Subject: %s' . PHP_EOL, $subject);
    printf('  Groups info:' . PHP_EOL);
    foreach ($xinfo->groups() as $group) {
        foreach ($group as $key => $value) {
            printf('    %s: %s' . PHP_EOL, $key, $value);
        }
        printf('    Consumers info:' . PHP_EOL);
        foreach ($xinfo->consumers($group['name']) as $consumer) {
            foreach ($consumer as $key => $value) {
                printf('      %s: %s' . PHP_EOL, $key, $value);
            }
        }
    }
    printf('Raw stream info:' . PHP_EOL, $subject);
    print_r($xinfo->stream(true, 10));
}
