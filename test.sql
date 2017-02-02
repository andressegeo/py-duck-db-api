SELECT   Sum(`user.contact.id`) AS 'TAMERE',
         `user.id`              AS `_id.id`
FROM     (
                SELECT *
                FROM   (
                              SELECT *
                              FROM   (
                                            SELECT `user`.`id`                        AS `user.id`,
                                                   `user`.`birth`                     AS `user.birth`,
                                                   `user.contact`.`id`                AS `user.contact.id`,
                                                   `user.contact`.`number`            AS `user.contact.number`,
                                                   `user.company`.`id`                AS `user.company.id`,
                                                   `user.contact.type`.`id`           AS `user.contact.type.id`,
                                                   `user.contact.type`.`name`         AS `user.contact.type.name`,
                                                   `user.company.contact`.`id`        AS `user.company.contact.id`,
                                                   `user.company.contact`.`number`    AS `user.company.contact.number`,
                                                   `user.company.contact.type`.`id`   AS `user.company.contact.type.id`,
                                                   `user.company.contact.type`.`name` AS `user.company.contact.type.name`
                                            FROM   `user`
                                            JOIN   `phone` AS `user.contact`
                                            ON     `user`.`contact` = `user.contact`.`id`
                                            JOIN   `company` AS `user.company`
                                            ON     `user`.`company` = `user.company`.`id`
                                            JOIN   `type` AS `user.contact.type`
                                            ON     `user.contact`.`type` = `user.contact.type`.`id`
                                            JOIN   `phone` AS `user.company.contact`
                                            ON     `user.company`.`contact` = `user.company.contact`.`id`
                                            JOIN   `type` AS `user.company.contact.type`
                                            ON     `user.company.contact`.`type` = `user.company.contact.type`.`id` )
                              AS s_0 )
                AS s_1 )
        AS s_2
GROUP BY `user.id`