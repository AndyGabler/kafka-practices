package io.github.andygabler.nfl.result.producer.gameresult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class GameResultController {

    private static final Logger LOGGER = LoggerFactory.getLogger(GameResultController.class);

    @Autowired
    private GameResultService gameResultService;

    @GetMapping("/gameResult")
    public ModelAndView gameResultForm(String userMessage){
        LOGGER.info("Game result form loaded.");
        final ModelAndView modelAndView = new ModelAndView("resultForm", "result", new GameResult());
        modelAndView.addObject("userMessage", userMessage);
        return modelAndView;
    }

    @PostMapping("/gameResult")
    public ModelAndView gameResultForm(
        @ModelAttribute("result")
        GameResult result
    ) {
        try {
            gameResultService.postResult(result);
            return gameResultForm("Submitted game result.");
        } catch (Exception exception) {
            LOGGER.error("Error submitting game result.", exception);
            return gameResultForm(exception.toString());
        }
    }
}
