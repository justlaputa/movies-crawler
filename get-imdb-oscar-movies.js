var movies = {}
var order = 0

function getLink(link) {
  link = link.substring(0, link.lastIndexOf('?'))
  return 'http://www.imdb.com' + link
}

function addMovie(category, title, link) {
  if (!movies[title]) {
    movies[title] = {}
    movies[title].nominees = []
    movies[title].order = order++
  }

  movies[title].link = link
  movies[title].nominees.push(category)
}

$('.nominationsCategory').each(function() {
  var $category = $(this)
  var categoryDescription = $category.find('.nominatedFor').text()

  $category.find('.nominee').each(function(){
    var $item = $(this)
    var link = $item.find('.list_item>a').attr('href')
    var title = null
    if (link.startsWith('/title/')) {
      title = $item.find('img').attr('title')
    } else if ($item.find('.details > .forTitleDesktop').length > 0) {
      title = $item.find('.details > .forTitleDesktop > a').text().trim()
    } else {
      return true
    }
    addMovie(categoryDescription, title, link)
  })
})
