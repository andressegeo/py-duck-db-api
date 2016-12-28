SELECT *
FROM
(
    SELECT
    `hour.issue` AS 'issue_formated',
    `hour.id`, `affected_to.email` AS 'user_email'
    FROM (
        SELECT *
        FROM
        (
            SELECT `hour`.`id` AS `hour.id`,
            `hour`.`issue` AS `hour.issue`,
            `hour`.`started_at` AS `hour.started_at`,
            `hour`.`minutes` AS `hour.minutes`,
            `hour`.`comments` AS `hour.comments`,
            `project`.`id` AS `project.id`,
            `project`.`name` AS `project.name`,
            `client`.`id` AS `client.id`,
            `client`.`name` AS `client.name`,
            `affected_to`.`id` AS `affected_to.id`,
            `affected_to`.`email` AS `affected_to.email`,
            `affected_to`.`name` AS `affected_to.name`
            FROM hour JOIN `project` AS `project` ON `hour`.`project` = `project`.`id`
            JOIN `client` AS `client` ON `project`.`client` = `client`.`id`
            JOIN `user` AS `affected_to` ON `hour`.`affected_to` = `affected_to`.`id`
        )
        AS s_0
    )
    AS s_1
) AS s_2
WHERE (issue_formated = 'test')
