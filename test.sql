SELECT `user`.`id` AS `user.id`, `user`.`birth` AS `user.birth`, `user`.`id` AS `user.id`, `user`.`birth` AS `user.birth`, `user.contact`.`id` AS `user.contact.id`, `user.contact`.`number` AS `user.contact.number`, `user.contact`.`id` AS `user.contact.id`, `user.contact`.`number` AS `user.contact.number`, `user.company`.`id` AS `user.company.id`, `user.company.contact`.`id` AS `user.company.contact.id`, `user.company.contact`.`number` AS `user.company.contact.number`, `user.company.contact`.`id` AS `user.company.contact.id`, `user.company.contact`.`number` AS `user.company.contact.number` FROM `user`
                JOIN `phone`
                AS `user.contact`
                ON `user`.`contact` = `user.contact`.`id`

                JOIN `company`
                AS `user.company`
                ON `user`.`company` = `user.company`.`id`

                JOIN `type`
                AS `user.contact.type`
                ON `user.contact`.`type` = `user.contact.type`.`id`

                JOIN `phone`
                AS `user.company.contact`
                ON `user.company`.`contact` = `user.company.contact`.`id`

                JOIN `type`
                AS `user.company.contact.type`
                ON `user.company.contact`.`type` = `user.company.contact.type`.`id`
