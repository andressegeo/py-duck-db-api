


SELECT
s_1.`hour.id` AS `hour.id`,  
s_1.`project.id` AS `project.id`,  
s_1.`client.id` AS `client.id`
FROM
(
    SELECT
    s_0.`hour.id` AS `hour.id`,  
    s_0.`project.id` AS `project.id`,  
    s_0.`client.id` AS `client.id`
    FROM
    (
        SELECT 
        `hour`.`id` AS `hour.id`,  
        `project`.`id` AS `project.id`,  
        `client`.`id` AS `client.id`
        FROM hour 
        JOIN `project` AS `project` 
        ON `hour`.`project` = `project`.`id` 
        JOIN `client` AS `client` 
        ON `project`.`client` = `client`.`id` 
        JOIN `user` AS `affected_to` 
        ON `hour`.`affected_to` = `affected_to`.`id` 
    )
    AS s_0
    WHERE s_0.`hour.id` > 0
) AS s_1
;
